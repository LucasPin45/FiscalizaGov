import datetime as dt
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dou  # noqa: E402


class TestOrgao(unittest.TestCase):
    def test_raiz_e_curto(self):
        h = "Ministério da Fazenda/Secretaria Especial da Receita Federal do Brasil/Superintendência X"
        self.assertEqual(dou.orgao_raiz(h), "Ministério da Fazenda")
        self.assertEqual(dou.orgao_curto(h), "Ministério da Fazenda › Superintendência X")
        self.assertEqual(dou.orgao_curto("Ministério da Saúde"), "Ministério da Saúde")
        self.assertEqual(dou.orgao_curto(""), "")

    def test_curto_trunca(self):
        self.assertLessEqual(len(dou.orgao_curto("A/" + "B" * 200, 50)), 50)


class TestNivel(unittest.TestCase):
    def test_faixas(self):
        self.assertEqual([dou.nivel_relevancia(x) for x in (75, 60, 59, 35, 34, 10, None)],
                         ["Alta", "Alta", "Média", "Média", "Baixa", "Baixa", "Baixa"])


class TestTopDiverso(unittest.TestCase):
    def test_no_maximo_2_por_raiz(self):
        linhas = [{"Órgão": "Fazenda/RFB", "Score": 90 - i} for i in range(4)]
        linhas += [{"Órgão": "Saúde/X", "Score": 50}, {"Órgão": "Educação/Y", "Score": 40}]
        top = dou.top_diverso(pd.DataFrame(linhas), 5, 2)
        raizes = [dou.orgao_raiz(o) for o in top["Órgão"]]
        self.assertEqual(raizes.count("Fazenda"), 2)
        self.assertEqual(len(top), 4)  # só existem 3 órgãos: 2 + 1 + 1


class TestUltimoDia(unittest.TestCase):
    def test_volta_ate_achar_edicao(self):
        cheio = pd.DataFrame([{c: "x" for c in dou.COLUNAS}])
        vazio = pd.DataFrame(columns=dou.COLUNAS)
        dias = {dt.date(2026, 11, 15): vazio, dt.date(2026, 11, 14): vazio, dt.date(2026, 11, 13): cheio}
        d, df, erros = dou.ultimo_dia_com_edicao(lambda x: (dias[x], []), dt.date(2026, 11, 15))
        self.assertEqual((d, len(df), erros), (dt.date(2026, 11, 13), 1, []))

    def test_erro_nao_e_mascarado_como_dia_vazio(self):
        vazio = pd.DataFrame(columns=dou.COLUNAS)
        d, df, erros = dou.ultimo_dia_com_edicao(lambda x: (vazio, ["HTTP 500"]), dt.date(2026, 11, 15))
        self.assertEqual(erros, ["HTTP 500"])
        self.assertEqual(d, dt.date(2026, 11, 15))


if __name__ == "__main__":
    unittest.main()
