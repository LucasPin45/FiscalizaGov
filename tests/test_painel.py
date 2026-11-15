import datetime as dt
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import orcamento  # noqa: E402
import painel as pn  # noqa: E402
import relogio  # noqa: E402
from dou import ErroColeta  # noqa: E402

CAB_REC = ["CÓDIGO ÓRGÃO SUPERIOR", "NOME ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO", "NOME ÓRGÃO", "CÓDIGO UNIDADE GESTORA",
           "NOME UNIDADE GESTORA", "CATEGORIA ECONÔMICA", "ORIGEM RECEITA", "ESPÉCIE RECEITA", "DETALHAMENTO",
           "VALOR PREVISTO ATUALIZADO", "VALOR LANÇADO", "VALOR REALIZADO", "PERCENTUAL REALIZADO", "DATA LANÇAMENTO",
           "ANO EXERCÍCIO"]


def br(v):
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def lin(cat, orig, det, valor, data):
    return ["1", "S", "1", "O", "1", "UG", cat, orig, "esp", det, "0,00", "0,00", br(valor), "0,00", data, "2026"]


def zip_rec(linhas, nome="2026_Receitas.csv"):
    csv = ";".join(f'"{c}"' for c in CAB_REC) + "\n" + "\n".join(";".join(f'"{x}"' for x in l) for l in linhas) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(nome, csv.encode("latin-1"))
    return buf.getvalue()


def base_receitas(ano="2026"):
    ls = [lin("Receitas Correntes", "Impostos, Taxas e Contribuições de Melhoria", "IRPJ", 100.0, f"05/01/{ano}"),
          lin("Receitas Correntes", "Contribuições", "COFINS", 200.0, f"10/01/{ano}"),
          lin("Receitas de Capital", "Operações de Crédito", "TIT.RESP.TN-REFIN.DIV.PUB.FED.MERC.INT.-PRINC", 1000.0, f"12/01/{ano}"),
          lin("Receitas de Capital", "Operações de Crédito", "TIT.RESP.TN-MERC.INT.,EXC.REF.DIV.PUB.-PRINC.", 300.0, f"12/01/{ano}"),
          lin("Receitas Correntes - intra-orçamentárias", "Contribuições", "INTRA", 50.0, f"15/01/{ano}"),
          lin("Receitas de Capital", "Outras Receitas de Capital", "OUTRAS", 40.0, f"20/01/{ano}"),
          lin("Receitas Correntes", "Contribuições", "COFINS", 60.0, f"25/02/{ano}")]
    # completa para passar do mínimo de 1000 linhas do leitor (linhas de valor zero)
    ls += [lin("Receitas Correntes", "Contribuições", "ZERO", 0.0, f"05/01/{ano}") for _ in range(1100)]
    return ls


class TestReceitas(unittest.TestCase):
    def test_separa_arrecadacao_de_credito_e_ignora_intra(self):
        df = pn.ler_receitas(zip_rec(base_receitas()), 2026)
        r = pn.somar_receitas(df)
        self.assertAlmostEqual(r["arrecadacao"], 100 + 200 + 40 + 60)   # sem crédito e sem intra
        self.assertAlmostEqual(r["credito"], 1300.0)
        self.assertAlmostEqual(r["refinanciamento"], 1000.0)

    def test_corte_por_data(self):
        df = pn.ler_receitas(zip_rec(base_receitas()), 2026)
        self.assertAlmostEqual(pn.somar_receitas(df, pd.Timestamp("2026-01-31"))["arrecadacao"], 340.0)

    def test_erros_viram_erro_de_coleta(self):
        with self.assertRaises(ErroColeta):
            pn.ler_receitas(b"nao e zip", 2026)
        with self.assertRaises(ErroColeta):
            pn.ler_receitas(zip_rec(base_receitas()[:5]), 2026)  # poucas linhas
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("x.txt", "a")
        with self.assertRaises(ErroColeta):
            pn.ler_receitas(buf.getvalue(), 2026)


def df_orc(liq_pessoal=500.0, liq_juros=100.0, liq_amort=900.0, liq_reserva=0.0):
    linhas = []
    for tipo, liq, emp in ((orcamento.T_PESSOAL, liq_pessoal, liq_pessoal + 10), (orcamento.T_JUROS, liq_juros, liq_juros),
                           (orcamento.T_AMORT, liq_amort, liq_amort), (orcamento.T_RESERVA, liq_reserva, 0.0)):
        linhas.append({"Ano": 2026, "Tipo de gasto": tipo, "Liquidado": liq, "Empenhado": emp})
    return pd.DataFrame(linhas)


class TestGastos(unittest.TestCase):
    def test_gasto_sem_amortizacao_e_com_juros(self):
        g = pn.somar_gastos(df_orc())
        self.assertEqual(g["gasto_liquidado"], 600.0)          # pessoal + juros; sem amortização
        self.assertEqual(g["juros_liquidado"], 100.0)
        self.assertEqual(g["amortizacao_liquidada"], 900.0)

    def test_reserva_nunca_entra(self):
        self.assertEqual(pn.somar_gastos(df_orc(liq_reserva=9999.0))["gasto_liquidado"], 600.0)


class TestMontagemERitmo(unittest.TestCase):
    def setUp(self):
        self.rec26 = pn.ler_receitas(zip_rec(base_receitas("2026")), 2026)
        self.rec25 = pn.ler_receitas(zip_rec(base_receitas("2025"), "2025_Receitas.csv"), 2025)

    def test_montar_asof_e_comparacao(self):
        p = pn.montar(2026, self.rec26, self.rec25, df_orc(), dt.date(2026, 3, 1), "x")
        self.assertEqual(p["receita"]["asof"], "2026-02-26")          # fim do último dia lançado (25/02)
        self.assertAlmostEqual(p["receita"]["arrecadacao"], 400.0)
        self.assertAlmostEqual(p["receita"]["mesmo_periodo_ano_anterior"], 400.0)  # 2025 até 25/02
        self.assertEqual(p["gasto"]["asof"], "2026-03-01")

    def test_ritmo_e_estimativa(self):
        ini = pn.inicio_do_ano_ms(2026)
        asof = ini + 10 * 86400 * 1000
        self.assertAlmostEqual(pn.ritmo(864000.0, ini, asof), 1.0)       # 864 mil em 10 dias = R$ 1/s
        self.assertAlmostEqual(pn.estimar(864000.0, ini, asof, asof + 60_000), 864060.0)
        self.assertEqual(pn.estimar(864000.0, ini, asof, asof - 5000), 864000.0)   # nunca estima para trás

    def test_snapshot_ida_e_volta(self):
        p = pn.montar(2026, self.rec26, None, df_orc(), None, "agora")
        with tempfile.TemporaryDirectory() as pasta:
            pn.salvar(p, pasta)
            self.assertEqual(pn.ler(pasta)["ano"], 2026)
        self.assertIsNone(pn.ler(tempfile.mkdtemp()))


class TestRelogioHtml(unittest.TestCase):
    def test_html_traz_dados_e_avisos(self):
        rec = pn.ler_receitas(zip_rec(base_receitas()), 2026)
        p = pn.montar(2026, rec, None, df_orc(), dt.date(2026, 3, 1), "x")
        h = relogio.html_relogio(p)
        self.assertIn("Relógio do Governo Federal em 2026", h)
        dados = json.loads(h.split("const D = ", 1)[1].split(";\nconst nf0", 1)[0])
        self.assertAlmostEqual(dados["rec"]["acum"], 400.0)
        self.assertAlmostEqual(dados["gas"]["acum"], 600.0)
        self.assertIn("estimativas", h)           # o aviso de que não é tempo real
        self.assertIn("setInterval(tick", h)


if __name__ == "__main__":
    unittest.main()
