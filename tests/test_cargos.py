import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import cargos  # noqa: E402
import dou  # noqa: E402

PREAMBULO = ("PORTARIA Nº 241, DE 6 DE NOVEMBRO DE 2026 O MINISTRO DE ESTADO DA SECRETARIA DE COMUNICAÇÃO SOCIAL "
             "DA PRESIDÊNCIA DA REPÚBLICA, no uso de suas atribuições e tendo em vista o Decreto nº 9.794, resolve: ")


def ato(corpo, orgao="Presidência da República/Casa Civil", titulo="PORTARIA Nº 1, DE 6 DE NOVEMBRO DE 2026", link="L1"):
    return {"Data": "13/11/2026", "Seção": "DOU 2", "Órgão": orgao, "Tipo": "Portaria", "Título": titulo,
            "Ementa/Resumo": (PREAMBULO + corpo)[:400], "Link": link, "Página": "1", "Edição": "1"}


class TestExtracao(unittest.TestCase):
    def test_nomeacao_com_nome_cargo_e_codigo(self):
        t = PREAMBULO + ("NOMEAR PABLO PEIXOTO DE SOUZA para exercer o cargo de Assessor, CCE 2.13, no Departamento de "
                         "Produção da Secretaria. SIDÔNIO CARDOSO PALMEIRA")
        m = cargos.extrair_movimentos(t)
        self.assertEqual(len(m), 1)
        self.assertEqual((m[0]["acao"], m[0]["nome"]), ("Nomeação", "PABLO PEIXOTO DE SOUZA"))
        self.assertTrue(m[0]["cargo"].startswith("Assessor"))
        self.assertEqual(m[0]["codigos"], ["CCE-2.13"])

    def test_nome_depois_de_clausulas_e_nome_misto(self):
        t = ("resolve: Exonerar, de acordo com o artigo 35, inciso I, da Lei nº 8.112, de 11 de dezembro de 1990, "
             "MARISA CARVALHO SUAREZ, Matrícula Siape nº 2702405, do cargo de Diretora de Programa, CCE 1.15.")
        self.assertEqual(cargos.extrair_movimentos(t)[0]["nome"], "MARISA CARVALHO SUAREZ")
        t2 = "resolve: Designar, a partir de 01/11/2026, a servidora Jéssica de Almeida Moreira, matrícula 10, para o encargo de substituta"
        self.assertEqual(cargos.extrair_movimentos(t2)[0]["nome"], "Jéssica de Almeida Moreira")

    def test_varias_movimentacoes_no_mesmo_ato(self):
        t = ("resolve: EXONERAR PHYDIA DE ATHAYDE do cargo de Diretora do Departamento X, CCE 1.15; "
             "NOMEAR GABRIELA MENDES para exercer o cargo de Diretora do Departamento X, CCE 1.15.")
        m = cargos.extrair_movimentos(t)
        self.assertEqual([x["acao"] for x in m], ["Exoneração", "Nomeação"])
        self.assertEqual([x["nome"] for x in m], ["PHYDIA DE ATHAYDE", "GABRIELA MENDES"])

    def test_dispensar_licitacao_nao_e_ato_de_pessoal(self):
        self.assertFalse(cargos.eh_ato_pessoal("resolve: dispensar a licitação para aquisição de material"))
        self.assertTrue(cargos.eh_ato_pessoal("resolve: DISPENSAR MARIA SILVA da função"))
        self.assertTrue(cargos.eh_ato_pessoal("Art. 1º Nomear, a pedido, JOSE SOUZA"))


class TestNivel(unittest.TestCase):
    def nivel(self, corpo, orgao="Presidência da República/Casa Civil"):
        a = cargos.analisar(pd.DataFrame([ato(corpo, orgao)]))
        return a.iloc[0]["Nível"], a.iloc[0]["Motivos"]

    def test_cce_1_17_e_alta_direcao(self):
        n, _ = self.nivel("NOMEAR MARIANA ZOCCOLI para exercer o cargo de Secretária de Produção, CCE 1.17, da Secom.")
        self.assertEqual(n, "Alta direção")

    def test_cce_1_15_e_das_5_alta_direcao(self):
        n, m = self.nivel("NOMEAR RAQUEL TEIXEIRA para exercer o cargo de Diretora de Departamento, CCE 1.15, da Secom.")
        self.assertEqual(n, "Alta direção")
        self.assertIn("CCE-1.15 (DAS 5)", m)

    def test_cce_1_13_e_das_4_intermediaria(self):
        n, m = self.nivel("NOMEAR LUIZA SANTOS para exercer o cargo de Coordenador-Geral, CCE 1.13, da Secom.")
        self.assertEqual(n, "Direção intermediária")
        self.assertIn("CCE-1.13 (DAS 4)", m)

    def test_nivel_14_e_intermediario_sem_das_equivalente(self):
        n, m = self.nivel("NOMEAR ANA PAULA LIMA para exercer o cargo de Chefe de Gabinete, CCE 1.14, da Secom.")
        self.assertEqual(n, "Direção intermediária")
        self.assertIn("entre DAS 4 e DAS 5", m)

    def test_nivel_10_das_3_nao_e_direcao(self):
        n, _ = self.nivel("NOMEAR EDILSON DANTAS para exercer o cargo de Coordenador de Projeto, CCE 3.10, da SRI.")
        self.assertEqual(n, "Demais")

    def test_assessor_em_secretaria_nacional_nao_e_secretario(self):
        n, _ = self.nivel("EXONERAR GIANCARLO GAMA do cargo de Assessor da Secretaria Nacional de Habitação, CCE 2.10.",
                          "Ministério das Cidades/Secretaria Nacional")
        self.assertEqual(n, "Demais")

    def test_preambulo_com_ministro_de_estado_nao_conta(self):
        n, _ = self.nivel("NOMEAR FULANO DA SILVA para exercer o cargo de Técnico, CCE 2.05, da Secom.")
        self.assertEqual(n, "Demais")

    def test_substituto_eventual_nao_e_alta_direcao(self):
        n, m = self.nivel("DESIGNAR JOSE OLIVEIRA para exercer o encargo de substituto eventual do cargo de Secretário, CCE 1.17.")
        self.assertEqual(n, "Direção intermediária")
        self.assertIn("substituição", m)

    def test_reitor_alta_mas_prorreitor_nao(self):
        n1, _ = self.nivel("NOMEAR JOSE SANTOS para exercer o cargo de Reitor da Universidade X.", "Atos do Poder Executivo")
        n2, _ = self.nivel("NOMEAR MARIA LIMA para exercer o cargo de Pró Reitora de Extensão.", "Ministério da Educação/Universidade X")
        self.assertEqual(n1, "Alta direção")
        self.assertNotEqual(n2, "Alta direção")

    def test_diretor_geral_de_campus_e_intermediario(self):
        n, _ = self.nivel("NOMEAR ANA COSTA para exercer o cargo de Diretora-Geral do Campus Y.",
                          "Ministério da Educação/Instituto Federal de X")
        self.assertEqual(n, "Direção intermediária")

    def test_decreto_presidencial_sempre_visivel(self):
        a = cargos.analisar(pd.DataFrame([ato("NOMEAR JOAO PEREIRA para exercer o cargo de Juiz do Tribunal.",
                                              "Atos do Poder Executivo", "DECRETO DE 9 DE NOVEMBRO DE 2026")]))
        r = a.iloc[0]
        self.assertTrue(r["Decreto presidencial"])
        self.assertEqual(r["Nível"], "Direção intermediária")
        self.assertIn("decreto do Presidente", r["Motivos"])


class TestEquivalenciaDas(unittest.TestCase):
    """Anexo III da Lei 14.204/2021: DAS-1=CCE-5, DAS-2=CCE-7, DAS-3=CCE-10, DAS-4=CCE-13, DAS-5=CCE-15,
    DAS-6=CCE-17, NE=CCE-18. O nível é o número depois do ponto."""

    def test_tabela_oficial(self):
        eq = {"5": "DAS 1", "7": "DAS 2", "10": "DAS 3", "13": "DAS 4", "15": "DAS 5", "17": "DAS 6", "18": "NE"}
        for nivel, das in eq.items():
            self.assertEqual(cargos.equivalente_das("CCE", f"1.{nivel}"), das)
            self.assertEqual(cargos.equivalente_das("FCE", f"1.{nivel}"), das)

    def test_serie_nao_muda_o_nivel(self):
        self.assertEqual(cargos.equivalente_das("CCE", "2.13"), "DAS 4")
        self.assertEqual(cargos.equivalente_das("CCE", "3.15"), "DAS 5")

    def test_rotulo_entre_parenteses(self):
        self.assertEqual(cargos.rotulo_codigo("CCE-1.13"), "CCE-1.13 (DAS 4)")
        self.assertEqual(cargos.rotulo_codigo("CCE-1.18"), "CCE-1.18 (NE)")
        self.assertEqual(cargos.rotulo_codigo("FCE-1.17"), "FCE-1.17 (DAS 6)")

    def test_niveis_sem_equivalente_exato(self):
        self.assertEqual(cargos.equivalente_das("CCE", "1.14"), "entre DAS 4 e DAS 5")
        self.assertEqual(cargos.equivalente_das("CCE", "1.16"), "entre DAS 5 e DAS 6")
        self.assertEqual(cargos.equivalente_das("CCE", "2.02"), "abaixo do DAS 1")

    def test_codigos_sem_equivalencia_ficam_iguais(self):
        self.assertEqual(cargos.rotulo_codigo("FCPE-1"), "FCPE-1")
        self.assertEqual(cargos.rotulo_codigo("DAS-101.4"), "DAS-101.4")

    def test_movimentos_df_mostra_equivalencia(self):
        a = cargos.analisar(pd.DataFrame([ato("NOMEAR LUIZA SANTOS para exercer o cargo de Coordenador-Geral, CCE 1.13, da Secom.")]))
        self.assertEqual(cargos.movimentos_df(a).iloc[0]["Códigos"], "CCE-1.13 (DAS 4)")


class TestSelecao(unittest.TestCase):
    def test_exclui_judiciario_e_mpu(self):
        df = pd.DataFrame([ato("NOMEAR FULANO DA SILVA para exercer o cargo de Analista.", "Poder Judiciário/TRT 2"),
                           ato("NOMEAR FULANO DA SILVA para exercer o cargo de Analista.", "Ministério Público da União/PGR"),
                           ato("NOMEAR FULANO DA SILVA para exercer o cargo de Analista.", "Ministério da Cultura/Gabinete")])
        self.assertEqual(len(cargos.analisar(df)), 1)

    def test_unidade_da_presidencia(self):
        self.assertEqual(cargos.unidade_presidencia("Presidência da República/Casa Civil/Secretaria Executiva"), "Casa Civil")
        self.assertEqual(cargos.unidade_presidencia("Atos do Poder Executivo"), "Atos do Presidente (decretos)")
        self.assertEqual(cargos.unidade_presidencia("Ministério da Saúde"), "")

    def test_movimentos_df_uma_linha_por_movimento(self):
        a = cargos.analisar(pd.DataFrame([ato("EXONERAR ANA SOUZA do cargo de Diretora, CCE 1.15; "
                                              "NOMEAR BIA LIMA para exercer o cargo de Diretora, CCE 1.15.")]))
        mv = cargos.movimentos_df(a)
        self.assertEqual(list(mv["Ação"]), ["Exoneração", "Nomeação"] if mv.iloc[0]["Ação"] == "Exoneração" else ["Nomeação", "Exoneração"])
        self.assertTrue(mv["Presidência"].all())
        self.assertEqual(set(mv["Nome"]), {"ANA SOUZA", "BIA LIMA"})

    def test_extrair_texto_da_pagina(self):
        h = '<div class="texto-dou"><p class="identifica">PORTARIA 1</p><p class="dou-paragraph">NOMEAR A&amp;B</p></div> <div class="x"></div>'
        self.assertEqual(cargos.extrair_texto(h), "PORTARIA 1 NOMEAR A&B")

    def test_vazio_nao_quebra(self):
        vazio = pd.DataFrame(columns=dou.COLUNAS)
        self.assertTrue(cargos.analisar(vazio).empty)
        self.assertTrue(cargos.movimentos_df(cargos.analisar(vazio)).empty)


if __name__ == "__main__":
    unittest.main()
