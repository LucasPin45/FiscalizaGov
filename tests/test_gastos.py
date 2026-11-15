import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import gastos  # noqa: E402


def linha(sup="Ministério da Educação", cod_sup="26000", org="FNDE", cod="26298",
          emp="1.000,00", liq="800,00", pago="600,00"):
    return {"ano": 2026, "orgao": org, "codigoOrgao": cod, "orgaoSuperior": sup,
            "codigoOrgaoSuperior": cod_sup, "empenhado": emp, "liquidado": liq, "pago": pago}


class Resp:
    def __init__(self, status, payload=None):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p


class Sessao:
    def __init__(self, por_codigo, status=200):
        self.por_codigo, self.status, self.chamadas = por_codigo, status, []

    def get(self, url, params=None, headers=None, **kw):
        self.chamadas.append(params)
        if self.status != 200:
            return Resp(self.status)
        todas = self.por_codigo.get(params["orgaoSuperior"], [])
        ini = (params["pagina"] - 1) * gastos.POR_PAGINA
        return Resp(200, todas[ini:ini + gastos.POR_PAGINA])


class TestParse(unittest.TestCase):
    def test_valor_br(self):
        self.assertEqual(gastos.valor_br("126.801.192.978,82"), 126801192978.82)
        self.assertEqual(gastos.valor_br("0,00"), 0.0)
        self.assertEqual(gastos.valor_br("lixo"), 0.0)

    def test_fmt_curto(self):
        self.assertEqual(gastos.fmt_brl_curto(126_801_192_978.82), "R$ 126,80 bi")
        self.assertEqual(gastos.fmt_brl_curto(15_700_000), "R$ 15,70 mi")
        self.assertEqual(gastos.fmt_brl_curto(2_500_000_000_000), "R$ 2,50 tri")


class TestColeta(unittest.TestCase):
    def test_pagina_ate_acabar_e_normaliza(self):
        muitas = [linha(org=f"O{i}", cod=str(i)) for i in range(20)]  # 2 paginas
        s = Sessao({"26000": muitas})
        df, erros = gastos.coletar_ano(2026, "K", s, codigos=["26000"], pausa=0)
        self.assertEqual((len(df), erros), (20, []))
        self.assertEqual(df.iloc[0]["Pago"], 600.0)
        self.assertEqual([c["pagina"] for c in s.chamadas], [1, 2])

    def test_chave_recusada_e_erro_e_para(self):
        df, erros = gastos.coletar_ano(2026, "ruim", Sessao({}, status=401), codigos=["26000", "36000"], pausa=0)
        self.assertTrue(df.empty)
        self.assertEqual(len(erros), 1)
        self.assertIn("chave", erros[0])

    @mock.patch("gastos.time.sleep")
    def test_falha_vira_erro_nunca_sem_gasto(self, _):
        df, erros = gastos.coletar_ano(2026, "K", Sessao({}, status=500), codigos=["26000"], pausa=0)
        self.assertTrue(df.empty)
        self.assertIn("HTTP 500", erros[0])
        self.assertIn("Ministério da Educação", erros[0])


class TestRanking(unittest.TestCase):
    def setUp(self):
        self.df = gastos.normalizar([
            linha(org="FNDE", cod="1", pago="600,00", emp="1.000,00"),
            linha(org="UFX", cod="2", pago="400,00", emp="500,00"),
            linha(sup="Ministério da Saúde", cod_sup="36000", org="FNS", cod="3", pago="1.500,00", emp="3.000,00"),
            linha(sup="Refinanciamento da Dívida", cod_sup="75000", org="Tesouro", cod="4",
                  pago="999.999,00", emp="999.999,00"),
        ])

    def test_encargos_ficam_fora_por_padrao(self):
        base = gastos.filtrar_finalisticos(self.df, incluir_encargos=False)
        self.assertNotIn("Refinanciamento da Dívida", set(base["Órgão superior"]))
        self.assertIn("Refinanciamento da Dívida", set(gastos.filtrar_finalisticos(self.df, True)["Órgão superior"]))

    def test_ranking_superiores_ordena_e_percentuais(self):
        base = gastos.filtrar_finalisticos(self.df, False)
        r = gastos.ranking_superiores(base, "Pago")
        self.assertEqual(r.iloc[0]["Órgão superior"], "Ministério da Saúde")
        self.assertAlmostEqual(r["% do total"].sum(), 100.0)
        self.assertAlmostEqual(r.iloc[0]["% pago do empenhado"], 50.0)
        self.assertAlmostEqual(gastos.concentracao(r, 1), 1500 / 2500 * 100)

    def test_ranking_orgaos(self):
        base = gastos.filtrar_finalisticos(self.df, False)
        r = gastos.ranking_orgaos(base, "Pago")
        self.assertEqual(list(r["Órgão"]), ["FNS", "FNDE", "UFX"])

    def test_vazio_nao_quebra(self):
        vazio = gastos.normalizar([])
        self.assertTrue(gastos.ranking_superiores(vazio, "Pago").empty)
        self.assertEqual(gastos.concentracao(gastos.ranking_superiores(vazio, "Pago"), 5), 0.0)


if __name__ == "__main__":
    unittest.main()
