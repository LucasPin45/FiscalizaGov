import datetime as dt
import os
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dou  # noqa: E402
import pncp  # noqa: E402


def contrato(**kw):
    base = {
        "orgaoEntidade": {"cnpj": "00394460000141", "razaoSocial": "MINISTERIO X", "poderId": "E", "esferaId": "F"},
        "unidadeOrgao": {"nomeUnidade": "Unidade A", "ufSigla": "DF"},
        "tipoContrato": {"nome": "Contrato (termo inicial)"}, "categoriaProcesso": {"nome": "Serviços"},
        "nomeRazaoSocialFornecedor": "EMPRESA LTDA", "niFornecedor": "11222333000144",
        "objetoContrato": "Serviço comum", "valorGlobal": 1000.0, "valorInicial": 1000.0, "valorAcumulado": None,
        "dataPublicacaoPncp": "2026-11-13T10:00:00", "dataVigenciaInicio": "2026-11-13",
        "dataVigenciaFim": "2027-11-13", "processo": "1/2026", "numeroControlePNCP": "N1",
        "anoContrato": 2026, "sequencialContrato": 7,
    }
    base.update(kw)
    return base


class Resp:
    def __init__(self, status, payload=None):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p


class Sessao:
    """Responde por numero de pagina; thread-safe o bastante para os testes."""
    def __init__(self, paginas, falhar=()):
        self.paginas, self.falhar = paginas, set(falhar)

    def get(self, url, params=None, **kw):
        p = params["pagina"]
        if p in self.falhar:
            return Resp(500)
        return Resp(200, {"data": self.paginas[p - 1], "totalPaginas": len(self.paginas)})


class TestColeta(unittest.TestCase):
    D = dt.date(2026, 11, 13)

    def test_paginas_e_filtro_federal_executivo(self):
        estadual = contrato(orgaoEntidade={"cnpj": "1", "razaoSocial": "EST", "poderId": "E", "esferaId": "E"})
        judiciario = contrato(orgaoEntidade={"cnpj": "2", "razaoSocial": "TRIB", "poderId": "J", "esferaId": "F"})
        s = Sessao([[contrato(), estadual], [judiciario, contrato(numeroControlePNCP="N2")]])
        df, erros = pncp.coletar_contratos(self.D, session=s)
        self.assertEqual((len(df), erros), (2, []))
        self.assertEqual(df.iloc[0]["Link"], "https://pncp.gov.br/app/contratos/00394460000141/2026/7")

    @mock.patch("pncp.time.sleep")
    def test_pagina_com_falha_vira_erro(self, _):
        s = Sessao([[contrato()], [contrato()], [contrato()]], falhar=[2])
        df, erros = pncp.coletar_contratos(self.D, session=s)
        self.assertEqual(len(df), 2)
        self.assertEqual(len(erros), 1)
        self.assertIn("página 2", erros[0])

    @mock.patch("pncp.time.sleep")
    def test_falha_total_vira_erro_nao_vazio_silencioso(self, _):
        s = Sessao([[contrato()]], falhar=[1])
        df, erros = pncp.coletar_contratos(self.D, session=s)
        self.assertTrue(df.empty)
        self.assertTrue(erros and "HTTP 500" in erros[0])

    def test_204_e_dia_sem_registros(self):
        class S204:
            def get(self, *a, **k):
                return Resp(204)
        df, erros = pncp.coletar_contratos(self.D, session=S204())
        self.assertEqual((len(df), erros), (0, []))


class TestRegras(unittest.TestCase):
    def avaliar(self, *itens):
        return pncp.avaliar_contratos(pncp.normalizar_contratos(list(itens)))

    def test_valor_muito_alto(self):
        r = self.avaliar(contrato(valorGlobal=20_000_000.0)).iloc[0]
        self.assertEqual(r["Score"], 45)
        self.assertIn("muito alto", r["Pontos de atenção"])

    def test_acrescimo_acima_de_25_por_cento(self):
        r = self.avaliar(contrato(valorInicial=100.0, valorAcumulado=200.0, valorGlobal=200.0)).iloc[0]
        self.assertIn("acréscimo de 100%", r["Pontos de atenção"])

    def test_emergencia(self):
        r = self.avaliar(contrato(objetoContrato="Contratação EMERGENCIAL de limpeza")).iloc[0]
        self.assertIn("emergência", r["Pontos de atenção"])

    def test_fracionamento_tres_no_mesmo_dia(self):
        itens = [contrato(numeroControlePNCP=f"N{i}") for i in range(3)]
        r = self.avaliar(*itens).iloc[0]
        self.assertIn("possível fracionamento", r["Pontos de atenção"])

    def test_contrato_comum_nao_dispara_nada(self):
        r = self.avaliar(contrato()).iloc[0]
        self.assertEqual((r["Score"], r["Pontos de atenção"]), (0, ""))

    def test_cpf_mascarado(self):
        self.assertEqual(pncp.mascarar_documento("12345678901"), "***.456.789-**")
        self.assertEqual(pncp.mascarar_documento("11222333000144"), "11222333000144")

    def test_dispensa_emergencia_e_inexigibilidade(self):
        base = {"orgaoEntidade": {"cnpj": "1", "razaoSocial": "M", "poderId": "E", "esferaId": "F"},
                "unidadeOrgao": {}, "objetoCompra": "obj", "valorTotalEstimado": 600_000.0,
                "dataPublicacaoPncp": "2026-11-13", "anoCompra": 2026, "sequencialCompra": 1,
                "numeroControlePNCP": "X", "situacaoCompraNome": "Divulgada"}
        e = dict(base, modalidadeNome="Dispensa",
                 amparoLegal={"nome": "Lei 14.133/2021, Art. 75, VIII", "descricao": "emergência"})
        i = dict(base, modalidadeNome="Inexigibilidade",
                 amparoLegal={"nome": "Lei 14.133/2021, Art. 74, III", "descricao": "notória especialização"})
        df = pncp.avaliar_dispensas(pncp.normalizar_dispensas([e, i]))
        pts = dict(zip(df["Modalidade"], df["Pontos de atenção"]))
        self.assertIn("emergência", pts["Dispensa"])
        self.assertIn("art. 74, III", pts["Inexigibilidade"])


class TestUltimoDiaUtil(unittest.TestCase):
    def test_domingo_vai_para_sexta(self):
        self.assertEqual(pncp.ultimo_dia_util(dt.date(2026, 11, 15)), dt.date(2026, 11, 13))
        self.assertEqual(pncp.ultimo_dia_util(dt.date(2026, 11, 16)), dt.date(2026, 11, 13))
        self.assertEqual(pncp.ultimo_dia_util(dt.date(2026, 11, 18)), dt.date(2026, 11, 17))


if __name__ == "__main__":
    unittest.main()
