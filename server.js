const express = require('express');
const cors = require('cors');
const pdfParse = require('pdf-parse');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;

// Increase body limit for PDF base64 (can be large)
app.use(express.json({ limit: '50mb' }));
app.use(cors());

// Supabase client
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// CNPJ → empresa_filial mapping
const EMPRESA_FILIAL_MAP = {
  '26602204000196': '1.01',
  '28036074000105': '2.01',
};

const IGNORED_DEST_CNPJS = new Set(['28036074000105']);

// ============================================
// Parse DANFE text extracted from PDF
// ============================================
function parseDanfeText(text) {
  const result = {
    chave_acesso: null,
    numero_nota: null,
    serie: null,
    data_emissao: null,
    natureza_operacao: null,
    emitente_cnpj: null,
    emitente_nome: null,
    destinatario_cnpj: null,
    destinatario_nome: null,
    valor_total: null,
    valor_produtos: null,
    valor_icms: null,
    modelo: null,
  };

  const clean = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  // 1. Chave de acesso (44 digits, may have spaces)
  const chavePatterns = [
    /(\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4})/,
    /CHAVE\s*DE\s*ACESSO[:\s]*\n?([\d\s]{44,60})/i,
    /(\d{44})/,
  ];
  for (const pat of chavePatterns) {
    const m = clean.match(pat);
    if (m) {
      const candidate = m[1].replace(/\s+/g, '');
      if (candidate.length === 44) {
        result.chave_acesso = candidate;
        break;
      }
    }
  }

  // Extract info from chave
  if (result.chave_acesso) {
    const chave = result.chave_acesso;
    result.emitente_cnpj = chave.substring(6, 20);
    result.serie = String(parseInt(chave.substring(22, 25)));
    result.numero_nota = String(parseInt(chave.substring(25, 34)));
    const mod = chave.substring(20, 22);
    if (mod === '55' || mod === '65') result.modelo = 'nfe_55';
    else if (mod === '62') result.modelo = 'nfcom_62';
    else if (mod === '57') result.modelo = 'cte_57';
  }

  // 2. Emitente nome — look for "RECEBEMOS DE ... OS PRODUTOS" pattern (most reliable)
  //    or the company name block before DANFE section
  const emitPatterns = [
    /RECEBEMOS\s+DE\s+(.+?)\s+OS\s+PRODUTOS/i,
    // Fallback: text block between address and "DANFE" or between header lines
    /CEP[:\s]*\d{5}[\-\s]?\d{3}\n(.+?)(?:\n\d|\nDANFE|\nNATUREZA)/is,
  ];
  for (const pat of emitPatterns) {
    const m = clean.match(pat);
    if (m && m[1].trim().length > 3) {
      // Clean up: remove line breaks, extra spaces
      let nome = m[1].replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
      // Remove trailing LTDA duplicates or junk
      result.emitente_nome = nome.substring(0, 100);
      break;
    }
  }

  // 3. Destinatario — extract from the DESTINATÁRIO section
  const destSection = clean.match(/DESTINAT[ÁA]RIO[\s\S]*?FATURA/i);
  if (destSection) {
    const destText = destSection[0];
    
    // Destinatario CNPJ: look for XX.XXX.XXX/XXXX-XX pattern in dest section
    const cnpjMatch = destText.match(/(\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2})/);
    if (cnpjMatch) {
      result.destinatario_cnpj = cnpjMatch[1].replace(/[.\/-]/g, '');
    }

    // Destinatario nome: find line containing the CNPJ (company name is on same line before CNPJ)
    if (cnpjMatch) {
      const cnpjLine = destText.split('\n').find(l => l.includes(cnpjMatch[1]));
      if (cnpjLine) {
        let nome = cnpjLine.replace(/\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}.*$/, '').trim();
        if (nome.length > 5) {
          result.destinatario_nome = nome.substring(0, 100);
        }
      }
    }
    
    // Fallback: look for known company names
    if (!result.destinatario_nome) {
      const knownMatch = destText.match(/(PRODUCTS\s+AND\s+FEATURES[^\n]*|BIOCOLLAGEN[^\n]*)/i);
      if (knownMatch) {
        let nome = knownMatch[1].replace(/\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}.*$/, '').trim();
        result.destinatario_nome = nome.substring(0, 100);
      }
    }
  }

  // 4. Data emissão — look for dates in DD/MM/YYYY format near "EMISSÃO"
  const dataPatterns = [
    // Date right after or near "DATA DA EMISSÃO" or "DATA DE EMISSÃO"
    /(\d{2}\/\d{2}\/\d{4})/,  // First date found is usually emissão
  ];
  // More specific: find date in the destinatario section (it's on the same line as CNPJ)
  const dataMatch = clean.match(/\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}\s+(\d{2}\/\d{2}\/\d{4})/);
  if (dataMatch) {
    const parts = dataMatch[1].split('/');
    if (parts.length === 3) {
      result.data_emissao = `${parts[2]}-${parts[1]}-${parts[0]}`;
    }
  }
  // Fallback: look for PROTOCOLO date
  if (!result.data_emissao) {
    const protMatch = clean.match(/PROTOCOLO[\s\S]*?(\d{2}\/\d{2}\/\d{4})/i);
    if (protMatch) {
      const parts = protMatch[1].split('/');
      if (parts.length === 3) {
        result.data_emissao = `${parts[2]}-${parts[1]}-${parts[0]}`;
      }
    }
  }

  // 5. Natureza da operação — line after "NATUREZA DA OPERAÇÃO"
  const natOpMatch = clean.match(/NATUREZA\s*DA\s*OPERA[ÇC][ÃA]O\n(.+)/i);
  if (natOpMatch) {
    const natOp = natOpMatch[1].trim();
    // Make sure it's not a header label
    if (natOp.length > 2 && !/^INSCRI/i.test(natOp)) {
      result.natureza_operacao = natOp.substring(0, 100);
    }
  }

  // 6. Valor total — multiple formats depending on document type
  
  // Format A: NFCOM — "VALOR TOTAL NFCOM...R$ 399,00R$ 399,00..."
  // The first R$ value after "VALOR TOTAL NFCOM" is the total
  if (!result.valor_total) {
    const nfcomMatch = clean.match(/VALOR\s*TOTAL\s*NFCOM[\s\S]*?R\$\s*([\d.,]+)/i);
    if (nfcomMatch) {
      const val = parseFloat(nfcomMatch[1].replace(/\./g, '').replace(',', '.'));
      if (val > 0) result.valor_total = val;
    }
  }

  // Format B: NF-e DANFE — "VALOR TOTAL DA NOTA" on one line, values on next line
  // Values may be glued: "0,000,000,000,000,00 1.560,00"
  if (!result.valor_total) {
    const notaSection = clean.match(/VALOR\s*TOTAL\s*DA\s*NOTA\n([^\n]+)/i);
    if (notaSection) {
      const numbers = notaSection[1].match(/\d[\d.,]*\d/g);
      if (numbers) {
        const lastNum = numbers[numbers.length - 1];
        const val = parseFloat(lastNum.replace(/\./g, '').replace(',', '.'));
        if (val > 0) result.valor_total = val;
      }
    }
  }

  // Format C: R$ prefix — look for "R$ X.XXX,XX" patterns near TOTAL
  if (!result.valor_total) {
    const rMatch = clean.match(/TOTAL[\s\S]{0,100}?R\$\s*([\d]+[.,][\d.,]+)/i);
    if (rMatch) {
      const val = parseFloat(rMatch[1].replace(/\./g, '').replace(',', '.'));
      if (val > 0) result.valor_total = val;
    }
  }

  // Format D: Fallback — any "VALOR TOTAL" followed by a number
  if (!result.valor_total) {
    const fallback = clean.match(/VALOR\s*TOTAL[\s\S]*?([\d]+\.[\d]{3},[\d]{2}|[\d]+,[\d]{2})/i);
    if (fallback) {
      const val = parseFloat(fallback[1].replace(/\./g, '').replace(',', '.'));
      if (val > 0) result.valor_total = val;
    }
  }

  // 7. Valor produtos — same approach, find last number after header
  const valorProdSection = clean.match(/VALOR\s*TOTAL\s*DOS\s*PRODUTOS[\s\S]*?\n([^\n]+)/i);
  if (valorProdSection) {
    const numbers = valorProdSection[1].match(/\d[\d.,]*\d/g);
    if (numbers) {
      const lastNum = numbers[numbers.length - 1];
      const val = parseFloat(lastNum.replace(/\./g, '').replace(',', '.'));
      if (val > 0) result.valor_produtos = val;
    }
  }

  // 8. ICMS — "VALOR DO ICMS" (not "VALOR DO ICMS ST")
  const icmsMatch = clean.match(/VALOR\s*DO\s*ICMS(?!\s*ST)\s*[\n\s]*([\d.,]+)/i);
  if (icmsMatch) {
    const val = parseFloat(icmsMatch[1].replace(/\./g, '').replace(',', '.'));
    if (val > 0) result.valor_icms = val;
  }

  return result;
}

// ============================================
// POST /api/parse-danfe-pdf
// ============================================
app.post('/api/parse-danfe-pdf', async (req, res) => {
  try {
    const { email_message_id, email_subject, email_from, email_from_name,
            email_received_at, pdf_base64, pdf_filename } = req.body;

    if (!email_message_id) {
      return res.status(400).json({ error: 'email_message_id is required' });
    }
    if (!pdf_base64) {
      return res.status(400).json({ error: 'pdf_base64 is required' });
    }

    // Clean pdf_base64: some attachments come with HTTP response headers prepended
    // Detect by decoding first bytes — real PDF starts with %PDF (JVBERi in base64)
    let cleanPdfBase64 = pdf_base64;
    if (!pdf_base64.startsWith('JVBERi')) {
      // Try to find the PDF start marker in the decoded content
      const rawBuffer = Buffer.from(pdf_base64, 'base64');
      const rawString = rawBuffer.toString('binary');
      const pdfStart = rawString.indexOf('%PDF');
      if (pdfStart > 0) {
        // Re-encode only the PDF part
        const pdfOnly = rawBuffer.slice(pdfStart);
        cleanPdfBase64 = pdfOnly.toString('base64');
        console.log(`Cleaned HTTP headers: removed ${pdfStart} bytes before %PDF`);
      } else {
        console.log('Warning: Could not find %PDF marker in content');
      }
    }

    // Check if record exists with XML already parsed
    const { data: existing } = await supabase
      .from('email_notas_fiscais')
      .select('id, tem_xml')
      .eq('email_message_id', email_message_id)
      .maybeSingle();

    if (existing && existing.tem_xml) {
      // XML already parsed, just update PDF info
      await supabase
        .from('email_notas_fiscais')
        .update({
          tem_pdf: true,
          pdf_filename: pdf_filename || null,
          updated_at: new Date().toISOString(),
        })
        .eq('id', existing.id);

      return res.json({
        success: true,
        id: existing.id,
        message: 'XML already exists, PDF info updated',
      });
    }

    // Parse PDF text
    const pdfBuffer = Buffer.from(cleanPdfBase64, 'base64');
    const pdfData = await pdfParse(pdfBuffer);
    const parsed = parseDanfeText(pdfData.text);

    // Build record
    const record = {
      email_message_id,
      email_subject: email_subject || null,
      email_from: email_from || null,
      email_from_name: email_from_name || null,
      email_received_at: email_received_at || null,
      modelo: parsed.modelo || 'sem_xml',
      tem_xml: false,
      tem_pdf: true,
      pdf_filename: pdf_filename || null,
      status: 'pendente',
      chave_acesso: parsed.chave_acesso,
      numero_nota: parsed.numero_nota,
      serie: parsed.serie,
      data_emissao: parsed.data_emissao,
      natureza_operacao: parsed.natureza_operacao,
      emitente_cnpj: parsed.emitente_cnpj,
      emitente_nome: parsed.emitente_nome,
      destinatario_cnpj: parsed.destinatario_cnpj,
      destinatario_nome: parsed.destinatario_nome,
      valor_total: parsed.valor_total || 0,
      valor_produtos: parsed.valor_produtos || 0,
      valor_icms: parsed.valor_icms || 0,
    };

    // Match empresa_filial
    if (parsed.destinatario_cnpj && EMPRESA_FILIAL_MAP[parsed.destinatario_cnpj]) {
      record.empresa_filial = EMPRESA_FILIAL_MAP[parsed.destinatario_cnpj];
    }

    // Mark as ignored if Biocollagen
    if (parsed.destinatario_cnpj && IGNORED_DEST_CNPJS.has(parsed.destinatario_cnpj)) {
      record.status = 'ignorada';
    }

    // Match fornecedor
    if (parsed.emitente_cnpj) {
      const { data: fornecedor } = await supabase
        .from('compras_entidades_cache')
        .select('codigo_entidade, nome')
        .eq('cnpj', parsed.emitente_cnpj)
        .maybeSingle();

      if (fornecedor) {
        record.fornecedor_codigo = fornecedor.codigo_entidade;
        record.fornecedor_match_auto = true;
        if (!record.emitente_nome) {
          record.emitente_nome = fornecedor.nome;
        }
      }
    }

    // Upsert
    let result;
    if (existing && !existing.tem_xml) {
      const { data: updated, error } = await supabase
        .from('email_notas_fiscais')
        .update({ ...record, updated_at: new Date().toISOString() })
        .eq('id', existing.id)
        .select('id, status, modelo, chave_acesso, emitente_nome, valor_total')
        .single();

      if (error) return res.status(500).json({ error: error.message });
      result = { success: true, ...updated, message: 'PDF record updated with parsed data' };
    } else {
      const { data: inserted, error } = await supabase
        .from('email_notas_fiscais')
        .insert(record)
        .select('id, status, modelo, chave_acesso, emitente_nome, valor_total')
        .single();

      if (error) return res.status(500).json({ error: error.message });
      result = { success: true, ...inserted, message: 'PDF record created with parsed data' };
    }

    return res.json(result);

  } catch (error) {
    console.error('Parse DANFE PDF error:', error);
    return res.status(500).json({
      error: 'Internal server error',
      details: error.message,
    });
  }
});

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'pef-pdf-parser', version: '1.0.0' });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log(`PDF Parser service running on port ${PORT}`);
});
