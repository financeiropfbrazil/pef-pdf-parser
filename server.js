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
    /CHAVE\s*DE\s*ACESSO[:\s]*\n?([\d\s]{44,60})/i,
    /(\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4}\s+\d{4})/,
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

  // 2. Emitente nome
  const emitPatterns = [
    /RAZ[ÃA]O\s*SOCIAL[:\s]*\n?\s*(.+)/i,
  ];
  for (const pat of emitPatterns) {
    const m = clean.match(pat);
    if (m && m[1].trim().length > 3) {
      result.emitente_nome = m[1].trim().substring(0, 100);
      break;
    }
  }

  // 3. Destinatario CNPJ (from the DESTINATÁRIO section)
  const destCnpjPatterns = [
    /DESTINAT[ÁA]RIO[\s\S]{0,500}?CNPJ[\/\s]*CPF[:\s]*\n?\s*([\d.\/\-]+)/i,
    /DESTINAT[ÁA]RIO[\s\S]{0,500}?([\d]{2}\.[\d]{3}\.[\d]{3}\/[\d]{4}-[\d]{2})/i,
  ];
  for (const pat of destCnpjPatterns) {
    const m = clean.match(pat);
    if (m) {
      result.destinatario_cnpj = m[1].replace(/[.\/-]/g, '');
      if (result.destinatario_cnpj.length === 14) break;
      result.destinatario_cnpj = null;
    }
  }

  // 4. Destinatario nome
  const destNomePatterns = [
    /DESTINAT[ÁA]RIO[\s\S]{0,200}?NOME[\/\s]*RAZ[ÃA]O\s*SOCIAL[:\s]*\n?\s*(.+)/i,
    /DESTINAT[ÁA]RIO[\s\S]{0,200}?\n\s*(PRODUCTS\s+AND\s+FEATURES[^\n]+)/i,
    /DESTINAT[ÁA]RIO[\s\S]{0,200}?\n\s*(BIOCOLLAGEN[^\n]+)/i,
  ];
  for (const pat of destNomePatterns) {
    const m = clean.match(pat);
    if (m && m[1].trim().length > 3) {
      result.destinatario_nome = m[1].trim().substring(0, 100);
      break;
    }
  }

  // 5. Data emissão
  const dataPatterns = [
    /DATA\s*(?:DA|DE)\s*EMISS[ÃA]O[:\s]*\n?\s*(\d{2}\/\d{2}\/\d{4})/i,
    /EMISS[ÃA]O[:\s]*(\d{2}\/\d{2}\/\d{4})/i,
  ];
  for (const pat of dataPatterns) {
    const m = clean.match(pat);
    if (m) {
      const parts = m[1].split('/');
      if (parts.length === 3) {
        result.data_emissao = `${parts[2]}-${parts[1]}-${parts[0]}`;
      }
      break;
    }
  }

  // 6. Valor total
  const valorPatterns = [
    /VALOR\s*TOTAL\s*DA\s*NOTA[:\s]*\n?\s*([\d.,]+)/i,
    /VALOR\s*TOTAL\s*(?:NF|NFCOM)[:\s]*\n?\s*R?\$?\s*([\d.,]+)/i,
  ];
  for (const pat of valorPatterns) {
    const m = clean.match(pat);
    if (m) {
      result.valor_total = parseFloat(m[1].replace(/\./g, '').replace(',', '.'));
      break;
    }
  }

  // 7. Valor produtos
  const valorProdMatch = clean.match(/VALOR\s*TOTAL\s*DOS\s*PRODUTOS[:\s]*\n?\s*([\d.,]+)/i);
  if (valorProdMatch) {
    result.valor_produtos = parseFloat(valorProdMatch[1].replace(/\./g, '').replace(',', '.'));
  }

  // 8. ICMS
  const icmsMatch = clean.match(/VALOR\s*(?:DO\s*)?ICMS[:\s]*\n?\s*([\d.,]+)/i);
  if (icmsMatch) {
    result.valor_icms = parseFloat(icmsMatch[1].replace(/\./g, '').replace(',', '.'));
  }

  // 9. Natureza da operação
  const natOpMatch = clean.match(/NATUREZA\s*DA\s*OPERA[ÇC][ÃA]O[:\s]*\n?\s*(.+)/i);
  if (natOpMatch && natOpMatch[1].trim().length > 2) {
    result.natureza_operacao = natOpMatch[1].trim().substring(0, 100);
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
    const pdfBuffer = Buffer.from(pdf_base64, 'base64');
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
