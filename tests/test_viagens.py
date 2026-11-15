import os
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import viagens as vg  # noqa: E402

CAB_VIAGEM = ["Identificador do processo de viagem", "Número da Proposta (PCDP)", "Situação", "Viagem Urgente",
              "Justificativa Urgência Viagem", "Código do órgão superior", "Nome do órgão superior",
              "Código órgão solicitante", "Nome órgão solicitante", "CPF viajante", "Nome", "Cargo", "Função",
              "Descrição Função", "Período - Data de início", "Período - Data de fim", "Destinos", "Motivo",
              "Valor diárias", "Valor passagens", "Valor devolução", "Valor outros gastos"]


def linha(id="1", situacao="Realizada", urgente="NÃO", just="Sem informação", cpf="***.111.222-**", nome="ANA SOUZA",
          ini="10/03/2026", fim="12/03/2026", destinos="Brasília/DF", diarias="900,00", passagens="1.000,00",
          devolucao="0,00", outros="0,00", sup="Ministério X", org="Órgão Y"):
    return [id, "P" + id, situacao, urgente, just, "26000", sup, "26001", org, cpf, nome, "ANALISTA", "FG-0001",
            "FG-0001", ini, fim, destinos, "Reunião", diarias, passagens, devolucao, outros]


def df_viagens(linhas):
    raw = pd.DataFrame(linhas, columns=CAB_VIAGEM).rename(columns=vg.COLS_VIAGEM)
    return vg.preparar_viagens(raw[list(vg.COLS_VIAGEM.values())])


def avaliado(linhas, intl=(), antec=None):
    v = df_viagens(linhas)
    return vg.avaliar(v, set(intl), pd.Series(antec or {}, dtype=float))


class TestPreparo(unittest.TestCase):
    def test_numeros_total_dias_e_flags(self):
        v = df_viagens([linha(diarias="1.500,50", passagens="2.000,00", devolucao="100,00", outros="10,00",
                              urgente="SIM", situacao="Não realizada")])
        r = v.iloc[0]
        self.assertAlmostEqual(r["total"], 1500.50 + 2000 + 10 - 100)
        self.assertEqual(r["dias"], 3)
        self.assertEqual(r["mes"], 3)
        self.assertTrue(r["eh_urgente"])
        self.assertFalse(r["realizada"])

    def test_sigilo(self):
        v = df_viagens([linha(nome="Informações protegidas por sigilo", cpf="Informações protegidas por sigilo")])
        self.assertTrue(v.iloc[0]["sigilo"])

    def test_internacional_e_antecedencia(self):
        t = pd.DataFrame({"id": ["1", "2", "3"], "pais_destino": ["Brasil", "Estados Unidos da América", "Sem informação"]})
        self.assertEqual(vg.viagens_internacionais(t), {"2"})
        v = df_viagens([linha(id="1", ini="10/03/2026")])
        p = pd.DataFrame({"id": ["1", "1"], "compra": ["01/03/2026", "09/03/2026"]})
        self.assertEqual(vg.antecedencia_compra(p, v).to_dict(), {"1": 1.0})  # a mais tardia manda


class TestRegras(unittest.TestCase):
    def pontos(self, **kw):
        a = avaliado([linha(**kw)], intl=kw.pop("intl", ()), antec=kw.pop("antec", None))
        return a.iloc[0]

    def test_viagem_comum_nao_pontua(self):
        r = avaliado([linha()]).iloc[0]
        self.assertEqual((r["score"], r["pontos"]), (0, ""))

    def test_valor_muito_alto_e_alto(self):
        self.assertIn("muito alto", avaliado([linha(passagens="120.000,00")]).iloc[0]["pontos"])
        r = avaliado([linha(passagens="40.000,00")]).iloc[0]
        self.assertIn("valor total alto", r["pontos"])
        self.assertEqual(r["score"], 25)

    def test_internacional_de_valor_alto(self):
        r = avaliado([linha(id="9", passagens="25.000,00")], intl={"9"}).iloc[0]
        self.assertIn("viagem internacional", r["pontos"])
        self.assertIn("internacional de valor alto", r["pontos"])

    def test_nao_realizada_sem_devolucao(self):
        r = avaliado([linha(situacao="Não realizada", devolucao="0,00")]).iloc[0]
        self.assertIn("sem devolução registrada", r["pontos"])
        r2 = avaliado([linha(situacao="Não realizada", devolucao="1.900,00")]).iloc[0]
        self.assertNotIn("sem devolução", r2["pontos"])

    def test_urgente_sem_justificativa(self):
        self.assertIn("urgente sem justificativa", avaliado([linha(urgente="SIM", just="")]).iloc[0]["pontos"])
        self.assertEqual(avaliado([linha(urgente="SIM", just="Evento de última hora")]).iloc[0]["pontos"], "")

    def test_passagem_depois_do_inicio_e_em_cima_da_hora(self):
        r = avaliado([linha(id="5")], antec={"5": -2.0}).iloc[0]
        self.assertIn("depois do início", r["pontos"])
        r2 = avaliado([linha(id="6")], antec={"6": 1.0}).iloc[0]
        self.assertIn("menos de 3 dias", r2["pontos"])
        r3 = avaliado([linha(id="7")], antec={"7": 20.0}).iloc[0]
        self.assertEqual(r3["pontos"], "")

    def test_viajante_frequente_e_viagem_longa(self):
        ls = [linha(id=str(i)) for i in range(vg.LIMITE_FREQUENTE)] + [linha(id="x", ini="01/01/2026", fim="15/03/2026")]
        a = avaliado(ls)
        self.assertTrue(a["pontos"].str.contains("ou mais viagens").all())
        self.assertIn("mais de 30 dias", a[a["id"] == "x"].iloc[0]["pontos"])

    def test_sigilo_nao_conta_como_viajante_frequente(self):
        ls = [linha(id=str(i), nome="Informações protegidas por sigilo", cpf="Informações protegidas por sigilo")
              for i in range(70)]
        self.assertFalse(avaliado(ls)["pontos"].str.contains("ou mais viagens").any())


class TestResumos(unittest.TestCase):
    def test_resumos(self):
        ls = [linha(id="1", destinos="Brasília/DF"), linha(id="2", destinos="Brasília/DF", urgente="SIM"),
              linha(id="3", cpf="***.999.999-**", nome="BIA LIMA", destinos="Manaus/AM", passagens="150.000,00"),
              linha(id="4", nome="Informações protegidas por sigilo", cpf="Informações protegidas por sigilo")]
        r = vg.resumir(avaliado(ls, intl={"3"}))
        self.assertEqual(sorted(r), sorted(vg.RECURSOS + vg.EXTRAS))
        org = r["orgao"].iloc[0]
        self.assertEqual((org["viagens"], org["urgentes"], org["internacionais"], org["viajantes"]), (4, 1, 1, 2))
        self.assertNotIn("Informações protegidas por sigilo", set(r["viajantes"]["nome"]))  # sigilo fora do ranking
        self.assertEqual(r["destinos"].iloc[0]["destinos"], "Brasília/DF")
        self.assertEqual(r["alertas"].iloc[0]["id"], "3")  # a mais pontuada abre a lista
        self.assertEqual(set(r["mes"]["mes"]), {3})


class TestListasCarasERecentes(unittest.TestCase):
    def setUp(self):
        ls = [linha(id="a", ini="05/03/2026", fim="06/03/2026", passagens="500,00"),
              linha(id="b", ini="20/03/2026", fim="21/03/2026", passagens="50.000,00"),
              linha(id="c", ini="10/03/2026", fim="11/03/2026", passagens="9.000,00"),
              linha(id="futura", ini="10/12/2026", fim="11/12/2026", passagens="99.000,00")]
        self.r = vg.resumir(avaliado(ls), hoje="2026-11-15")

    def test_mais_caras_ordenadas_por_valor(self):
        self.assertEqual(list(self.r["caras"]["id"]), ["futura", "b", "c", "a"])

    def test_mais_recentes_por_data_e_sem_futuras(self):
        self.assertEqual(list(self.r["recentes"]["id"]), ["b", "c", "a"])  # a viagem de dezembro ainda não ocorreu

    def test_leitura_tolera_arquivos_antigos_sem_as_listas(self):
        with tempfile.TemporaryDirectory() as pasta:
            base_apenas = {k: self.r[k] for k in vg.RECURSOS}
            vg.salvar(base_apenas, 2026, pasta=pasta, agora="x")
            dados, _ = vg.ler(2026, base=pasta)
            self.assertNotIn("caras", dados)
            vg.salvar(self.r, 2026, pasta=pasta, agora="x")
            dados2, _ = vg.ler(2026, base=pasta)
            self.assertIn("recentes", dados2)


class TestArquivos(unittest.TestCase):
    def _csv(self, pasta, nome, cab, linhas):
        with open(os.path.join(pasta, nome), "w", encoding="latin-1", newline="") as f:
            f.write(";".join(f'"{c}"' for c in cab) + "\n")
            for l in linhas:
                f.write(";".join(f'"{x}"' for x in l) + "\n")

    def test_processar_pasta_e_ida_e_volta_do_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._csv(tmp, "2026_Viagem.csv", CAB_VIAGEM, [linha(id="1"), linha(id="2", passagens="130.000,00")])
            # o arquivo real traz espaço sobrando no nome da primeira coluna do Trecho
            self._csv(tmp, "2026_Trecho.csv", ["Identificador do processo de viagem ", "Destino - País"],
                      [["1", "Brasil"], ["2", "Portugal"]])
            self._csv(tmp, "2026_Passagem.csv", ["Identificador do processo de viagem", "Data da emissão/compra"],
                      [["1", "01/02/2026"], ["2", "11/03/2026"]])
            r = vg.processar(tmp, 2026)
            self.assertEqual(int(r["orgao"]["viagens"].sum()), 2)
            self.assertEqual(int(r["orgao"]["internacionais"].sum()), 1)
            alerta = r["alertas"].iloc[0]
            self.assertEqual(alerta["id"], "2")
            self.assertIn("passagem emitida depois do início", alerta["pontos"])

            pasta = os.path.join(tmp, "out")
            vg.salvar(r, 2026, pasta=pasta, agora="15/11/2026 06:00", fonte="x")
            lido = vg.ler(2026, base=pasta)
            self.assertIsNotNone(lido)
            dados, meta = lido
            self.assertEqual(meta["gerado_em"], "15/11/2026 06:00")
            self.assertEqual(dados["orgao"].iloc[0]["cod_sup"], "26000")  # código como texto
            self.assertIsNone(vg.ler(2025, base=pasta))

    def test_coluna_faltando_levanta(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._csv(tmp, "2026_Viagem.csv", ["A", "B"], [["1", "2"]])
            with self.assertRaises(ValueError):
                vg.ler_csv(os.path.join(tmp, "2026_Viagem.csv"), vg.COLS_VIAGEM)


if __name__ == "__main__":
    unittest.main()
