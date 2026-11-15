import datetime as dt
import os
import sys
import tempfile
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import repasses as rp  # noqa: E402
from dou import ErroColeta  # noqa: E402

HOJE = dt.date(2026, 11, 15)


def item(id=1, situacao="EM EXECUÇÃO", valor=1_000_000.0, liberado=1_000_000.0, ini="2024-01-01", fim="2027-01-01",
         ultima="2026-06-01", objeto="CONSTRUCAO DE QUADRA POLIESPORTIVA", instr="CONTRATO DE REPASSE",
         ibge=4205407, mun="FLORIANOPOLIS", orgao="Ministério do Esporte"):
    return {
        "id": id, "dataInicioVigencia": ini, "dataFinalVigencia": fim, "dataUltimaLiberacao": ultima,
        "dataConclusao": None, "dimConvenio": {"objeto": objeto, "numero": f"N{id}"}, "situacao": situacao,
        "convenente": {"nome": f"MUNICIPIO DE {mun}", "tipo": "Administração Pública Municipal", "cnpjFormatado": "00.000.000/0001-00"},
        "municipioConvenente": {"codigoIBGE": ibge, "nomeIBGE": mun},
        "orgao": {"nome": orgao + " - Unidades com vínculo direto", "orgaoMaximo": {"nome": orgao}},
        "subfuncao": {"descricaoSubfuncap": "Desporto comunitário"},
        "tipoInstrumento": {"descricao": instr}, "valor": valor, "valorLiberado": liberado, "valorContrapartida": 1000.0,
    }


def avaliado(*itens, uf="SC"):
    return rp.avaliar(rp.normalizar(list(itens), uf), HOJE)


class TestClassificacao(unittest.TestCase):
    def test_grupo_da_situacao(self):
        g = rp.grupo_situacao
        self.assertEqual(g("EM EXECUÇÃO"), "andamento")
        self.assertEqual(g("ADIMPLENTE"), "andamento")
        self.assertEqual(g("CONCLUÍDO"), "encerrado")
        self.assertEqual(g("PRESTAÇÃO DE CONTAS APROVADA"), "encerrado")
        self.assertEqual(g("AGUARDANDO PRESTAÇÃO DE CONTAS"), "prestação de contas")
        self.assertEqual(g("INADIMPLENTE"), "problema")
        self.assertEqual(g("PRESTAÇÃO DE CONTAS REJEITADA"), "problema")

    def test_obra_por_instrumento_e_por_objeto(self):
        self.assertTrue(rp.eh_obra("CONTRATO DE REPASSE", "AQUISICAO DE EQUIPAMENTOS"))
        self.assertTrue(rp.eh_obra("CONVENIO", "PAVIMENTAÇÃO DA RUA X"))
        self.assertTrue(rp.eh_obra("CONVENIO", "Ampliação da escola"))
        self.assertFalse(rp.eh_obra("CONVENIO", "AQUISICAO DE MATERIAIS DE CONSUMO PARA UNIDADE DE SAUDE"))

    def test_normaliza_campos(self):
        d = rp.normalizar([item()], "SC")
        r = d.iloc[0]
        self.assertEqual((r["uf"], r["municipio"], r["cod_ibge"], r["instrumento"]),
                         ("SC", "Florianopolis", "4205407", "CONTRATO DE REPASSE"))
        self.assertEqual(r["orgao_superior"], "Ministério do Esporte")
        self.assertTrue(r["obra"])


class TestRegras(unittest.TestCase):
    def pontos(self, **kw):
        return avaliado(item(**kw)).iloc[0]

    def test_instrumento_em_dia_nao_pontua(self):
        r = self.pontos()
        self.assertEqual((r["score"], r["pontos"]), (0, ""))

    def test_vigencia_vencida_ha_mais_de_um_ano(self):
        r = self.pontos(fim="2025-06-01")
        self.assertEqual(r["score"], 40)
        self.assertIn("vigência encerrada há", r["pontos"])

    def test_vigencia_vencida_recente(self):
        r = self.pontos(fim="2026-09-01")
        self.assertEqual(r["score"], 25)

    def test_vencido_mas_concluido_nao_pontua(self):
        self.assertEqual(self.pontos(fim="2025-06-01", situacao="CONCLUÍDO")["score"], 0)

    def test_baixa_liberacao_com_prazo_avancado(self):
        r = self.pontos(ini="2024-01-01", fim="2027-03-01", liberado=100_000.0)
        self.assertIn("só 10% do valor liberado", r["pontos"])

    def test_nenhuma_liberacao_com_metade_do_prazo(self):
        r = self.pontos(liberado=0.0, ultima="")
        self.assertIn("nenhuma liberação", r["pontos"])

    def test_inadimplente_e_prestacao_rejeitada(self):
        self.assertIn("inadimplente", self.pontos(situacao="INADIMPLENTE")["pontos"])
        self.assertIn("rejeitada", self.pontos(situacao="PRESTAÇÃO DE CONTAS REJEITADA", fim="2026-01-01")["pontos"])

    def test_prestacao_de_contas_atrasada(self):
        r = self.pontos(situacao="AGUARDANDO PRESTAÇÃO DE CONTAS", fim="2026-05-01")
        self.assertIn("prestação de contas ainda não enviada", r["pontos"])
        self.assertEqual(self.pontos(situacao="AGUARDANDO PRESTAÇÃO DE CONTAS", fim="2026-11-01")["score"], 0)

    def test_valor_alto(self):
        self.assertIn("valor alto", self.pontos(valor=20_000_000.0, liberado=20_000_000.0)["pontos"])


class TestResumos(unittest.TestCase):
    def setUp(self):
        sc = [item(1), item(2, fim="2025-01-01"), item(3, ibge=4202008, mun="BLUMENAU", fim="2025-06-01"),
              item(4, situacao="CONCLUÍDO")]
        pr = [item(5), item(6, fim="2025-03-01")]
        self.d = rp.avaliar(pd.concat([rp.normalizar(sc, "SC"), rp.normalizar(pr, "PR")], ignore_index=True), HOJE)

    def test_resumo_por_uf(self):
        u = rp.resumo_uf(self.d).set_index("uf")
        self.assertEqual(int(u.loc["SC", "instrumentos"]), 4)
        self.assertEqual(int(u.loc["SC", "ativos"]), 3)       # o concluído sai da carteira ativa
        self.assertEqual(int(u.loc["SC", "vencidos"]), 2)
        self.assertAlmostEqual(u.loc["SC", "pct_vencidos"], 2 / 3 * 100)
        self.assertEqual(int(u.loc["PR", "vencidos"]), 1)

    def test_resumir_e_marca_ufs_completas(self):
        r = rp.resumir(self.d, ["SC"])
        self.assertEqual(sorted(r), sorted(rp.RECURSOS))
        flags = dict(zip(r["uf"]["uf"], r["uf"]["completa"]))
        self.assertEqual(flags, {"SC": True, "PR": False})
        self.assertTrue((r["sc"]["uf"] == "SC").all())
        self.assertEqual(set(r["municipios_sc"]["municipio"]), {"Florianopolis", "Blumenau"})
        self.assertEqual(r["alertas"].iloc[0]["score"], r["alertas"]["score"].max())

    def test_snapshot_ida_e_volta(self):
        r = rp.resumir(self.d, ["SC", "PR"])
        with tempfile.TemporaryDirectory() as pasta:
            rp.salvar(r, pasta=pasta, agora="15/11/2026 01:00", ufs_completas=["SC", "PR"], janela_txt="x")
            lido = rp.ler(pasta)
            self.assertIsNotNone(lido)
            dados, meta = lido
            self.assertEqual(meta["gerado_em"], "15/11/2026 01:00")
            self.assertEqual(dados["sc"].iloc[0]["cod_ibge"][:2], "42")
        self.assertIsNone(rp.ler(tempfile.mkdtemp()))


class Resp:
    def __init__(self, status, payload=None):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p


class Sessao:
    def __init__(self, paginas, status=200):
        self.paginas, self.status, self.chamadas = paginas, status, []

    def get(self, url, params=None, **kw):
        self.chamadas.append(params)
        if self.status != 200:
            return Resp(self.status)
        i = params["pagina"] - 1
        return Resp(200, self.paginas[i] if i < len(self.paginas) else [])


class TestColeta(unittest.TestCase):
    def test_janela_comeca_no_ano_anterior(self):
        self.assertEqual(rp.janela(HOJE)[0], "01/01/2025")

    def test_pagina_ate_acabar(self):
        pg1 = [item(i) for i in range(15)]
        pg2 = [item(100 + i) for i in range(4)]
        s = Sessao([pg1, pg2])
        itens = rp.coletar_uf("SC", "K", s, HOJE, pausa=0)
        self.assertEqual(len(itens), 19)
        self.assertEqual(s.chamadas[0]["uf"], "SC")
        self.assertEqual(s.chamadas[0]["dataVigenciaInicial"], "01/01/2025")

    @mock.patch("repasses.time.sleep")
    def test_falha_vira_erro_nunca_uf_vazia(self, _):
        with self.assertRaises(ErroColeta):
            rp.coletar_uf("SC", "K", Sessao([], status=500), HOJE, pausa=0)

    def test_chave_recusada(self):
        with self.assertRaises(ErroColeta) as cm:
            rp.coletar_uf("SC", "ruim", Sessao([], status=401), HOJE, pausa=0)
        self.assertIn("chave", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
