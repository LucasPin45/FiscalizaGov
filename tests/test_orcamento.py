import datetime as dt
import io
import os
import sys
import unittest
import zipfile

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import orcamento as o  # noqa: E402
from dou import ErroColeta  # noqa: E402

CAB = ["EXERCÍCIO", "CÓDIGO ÓRGÃO SUPERIOR", "NOME ÓRGÃO SUPERIOR", "CÓDIGO ÓRGÃO SUBORDINADO", "NOME ÓRGÃO SUBORDINADO",
       "CÓDIGO UNIDADE ORÇAMENTÁRIA", "NOME UNIDADE ORÇAMENTÁRIA", "CÓDIGO FUNÇÃO", "NOME FUNÇÃO", "CÓDIGO SUBFUNÇÃO",
       "NOME SUBFUNÇÃO", "CÓDIGO PROGRAMA ORÇAMENTÁRIO", "NOME PROGRAMA ORÇAMENTÁRIO", "CÓDIGO AÇÃO", "NOME AÇÃO",
       "CÓDIGO CATEGORIA ECONÔMICA", "NOME CATEGORIA ECONÔMICA", "CÓDIGO GRUPO DE DESPESA", "NOME GRUPO DE DESPESA",
       "CÓDIGO ELEMENTO DE DESPESA", "NOME ELEMENTO DE DESPESA", "ORÇAMENTO INICIAL (R$)", "ORÇAMENTO ATUALIZADO (R$)",
       "ORÇAMENTO EMPENHADO (R$)", "ORÇAMENTO REALIZADO (R$)", "% REALIZADO"]


def br(v):
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def linha(sup="26000", nsup="Ministério da Educação", org="26298", norg="FNDE", gnd="3", ngnd="Outras Despesas Correntes",
          el="39", nel="Serviços", dot=1000.0, emp=900.0, real=800.0, funcao="Educação"):
    return ["2026", sup, nsup, org, norg, "u", "UO", "12", funcao, "361", "sf", "p", "prog", "a", "acao", "3", "CORRENTES",
            gnd, ngnd, el, nel, br(dot), br(dot), br(emp), br(real), "0%"]


def zip_csv(linhas, nome="2026_OrcamentoDespesa.csv"):
    csv = ";".join(f'"{c}"' for c in CAB) + "\n" + "\n".join(";".join(f'"{x}"' for x in l) for l in linhas) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(nome, csv.encode("latin-1"))
    return buf.getvalue()


def base_linhas():
    # elementos 11 a 50 (nunca 54, usado no teste de agregação)
    return [linha(el=str(11 + i % 40), nel=f"El{i % 40}", dot=10.0, emp=5.0, real=4.0) for i in range(1100)]


def num(txt):
    return float(txt.replace(".", "").replace(",", "."))


class TestTipoDeGasto(unittest.TestCase):
    def test_mapeamento(self):
        t = o.tipo_de_gasto
        self.assertEqual(t("1", "11"), o.T_PESSOAL)
        self.assertEqual(t("1", "01"), o.T_APOSENT_SERV)
        self.assertEqual(t("3", "54"), o.T_INSS)
        self.assertEqual(t("3", "06"), o.T_SOCIAL)
        self.assertEqual(t("3", "48"), o.T_SOCIAL)
        self.assertEqual(t("3", "81"), o.T_TRANSF)
        self.assertEqual(t("3", "41"), o.T_TRANSF)
        self.assertEqual(t("3", "39"), o.T_CUSTEIO)
        self.assertEqual(t("4", "51"), o.T_INVEST)
        self.assertEqual(t("5", "65"), o.T_INVERSOES)
        self.assertEqual(t("2", "21"), o.T_JUROS)
        self.assertEqual(t("6", "71"), o.T_AMORT)
        self.assertEqual(t("9", "99"), o.T_RESERVA)
        self.assertEqual(t("3", "6"), o.T_SOCIAL)  # zero à esquerda perdido no CSV


class TestLeitura(unittest.TestCase):
    def test_le_agrega_e_classifica(self):
        ls = base_linhas() + [linha(el="54", nel="Aposentadorias do RGPS", dot=100.0, emp=90.0, real=80.0),
                              linha(el="54", nel="Aposentadorias do RGPS", dot=50.0, emp=40.0, real=30.0)]
        df = o.ler_zip(zip_csv(ls), 2026)
        inss = df[df["Cód. elemento"] == "54"]
        self.assertEqual(len(inss), 1)  # agregou as duas linhas
        self.assertEqual(inss.iloc[0]["Tipo de gasto"], o.T_INSS)
        self.assertEqual(inss.iloc[0]["Dotação atualizada"], 150.0)
        self.assertEqual(inss.iloc[0]["Empenhado"], 130.0)
        self.assertEqual(inss.iloc[0]["Liquidado"], 110.0)
        self.assertEqual(list(df.columns), o.COLUNAS)

    def test_zip_invalido_e_erro(self):
        with self.assertRaises(ErroColeta):
            o.ler_zip(b"nao e zip", 2026)

    def test_poucas_linhas_e_erro(self):
        with self.assertRaises(ErroColeta):
            o.ler_zip(zip_csv([linha()]), 2026)

    def test_csv_ausente_e_erro(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("outro.txt", "x")
        with self.assertRaises(ErroColeta):
            o.ler_zip(buf.getvalue(), 2026)

    def test_formato_inesperado_e_erro(self):
        csv = '"A";"B"\n' + "\n".join('"1";"2"' for _ in range(1100))
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("2026_OrcamentoDespesa.csv", csv.encode("latin-1"))
        with self.assertRaises(ErroColeta):
            o.ler_zip(buf.getvalue(), 2026)


class TestAnalises(unittest.TestCase):
    def setUp(self):
        ls = [
            linha(norg="A", org="1", gnd="1", ngnd="Pessoal", el="11", nel="Vencimentos", dot=1000, emp=950, real=900),
            linha(norg="A", org="1", gnd="4", ngnd="Investimentos", el="51", nel="Obras", dot=500, emp=100, real=50),
            linha(norg="B", org="2", gnd="4", ngnd="Investimentos", el="51", nel="Obras", dot=400, emp=380, real=300),
            linha(norg="C", org="3", gnd="3", ngnd="Outras", el="39", nel="Serviços", dot=50, emp=0, real=0),
            linha(norg="Reserva", org="9", gnd="9", ngnd="Reserva de Contingência", el="99", nel="Reserva", dot=9000, emp=0, real=0),
            linha(norg="Tesouro", org="25000", gnd="6", ngnd="Amortização", el="71", nel="Principal", dot=5000, emp=4000, real=4000),
        ]
        self.df = pd.DataFrame([dict(zip(o.COLUNAS, [
            2026, l[1], l[2], l[3], l[4], l[8], l[17], l[18], l[19], l[20], o.tipo_de_gasto(l[17], l[19]),
            num(l[21]), num(l[22]), num(l[23]), num(l[24])])) for l in ls])

    def test_reserva_sempre_fora_e_divida_so_se_pedir(self):
        d = o.sem_divida(self.df, False)
        self.assertNotIn(o.T_RESERVA, set(d["Tipo de gasto"]))
        self.assertNotIn(o.T_AMORT, set(d["Tipo de gasto"]))
        d2 = o.sem_divida(self.df, True)
        self.assertIn(o.T_AMORT, set(d2["Tipo de gasto"]))
        self.assertNotIn(o.T_RESERVA, set(d2["Tipo de gasto"]))

    def test_por_tipo_percentuais(self):
        t = o.por_tipo(o.sem_divida(self.df, False), "Empenhado")
        self.assertAlmostEqual(t["% do total"].sum(), 100.0)
        self.assertEqual(t.iloc[0]["Tipo de gasto"], o.T_PESSOAL)

    def test_execucao_ordem_e_percentuais(self):
        r = o.execucao(self.df, "Órgão", "Todas as despesas", dotacao_min=0)
        self.assertEqual(r.iloc[0]["Órgão"], "C")  # 0% empenhado
        a = r[r["Órgão"] == "A"].iloc[0]
        self.assertAlmostEqual(a["% empenhado"], 1050 / 1500 * 100)
        self.assertAlmostEqual(a["Não empenhado (R$)"], 450)
        self.assertNotIn("Reserva", set(r["Órgão"]))
        self.assertNotIn("Tesouro", set(r["Órgão"]))

    def test_execucao_so_investimentos_e_dotacao_minima(self):
        r = o.execucao(self.df, "Órgão", "Só investimentos (obras e equipamentos)", dotacao_min=450)
        self.assertEqual(list(r["Órgão"]), ["A"])
        self.assertAlmostEqual(r.iloc[0]["% empenhado"], 20.0)

    def test_execucao_por_ministerio(self):
        r = o.execucao(self.df, "Órgão superior", "Todas as despesas", dotacao_min=0)
        self.assertEqual(r.columns[0], "Órgão superior")
        self.assertEqual(len(r), 1)

    def test_detalhar_elementos(self):
        d = o.detalhar(self.df, o.T_INVEST, "Empenhado")
        self.assertEqual(list(d["Elemento"]), ["Obras"])
        self.assertAlmostEqual(d["% do tipo"].sum(), 100.0)

    def test_fracao_do_ano(self):
        self.assertEqual(o.fracao_ano(2025, dt.date(2026, 6, 1)), 100.0)
        self.assertEqual(o.fracao_ano(2027, dt.date(2026, 6, 1)), 0.0)
        self.assertAlmostEqual(o.fracao_ano(2026, dt.date(2026, 7, 2)), 183 / 365 * 100, places=3)


if __name__ == "__main__":
    unittest.main()
