import datetime as dt
import os
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import servidores as sv  # noqa: E402
import tab_viagens as tv  # noqa: E402
import atualizar_cargos_viajantes as script  # noqa: E402

CAB = ["Id_SERVIDOR_PORTAL", "NOME", "CPF", "MATRICULA", "DESCRICAO_CARGO", "SIGLA_FUNCAO", "NIVEL_FUNCAO", "FUNCAO",
       "ATIVIDADE", "ORG_LOTACAO", "ORGSUP_LOTACAO", "TIPO_VINCULO", "SITUACAO_VINCULO"]


def linha(nome, cpf, cargo="Sem informação", sigla="-1", nivel="-1", funcao="Sem informação", ativ="Sem informaç",
          org="Ministério X", sup="Ministério X", tipo="Cargo", sit="ATIVO PERMANENTE"):
    return ["1", nome, cpf, "123****", cargo, sigla, nivel, funcao, ativ, org, sup, tipo, sit]


def csv(pasta, linhas, nome="cad.csv"):
    caminho = os.path.join(pasta, nome)
    with open(caminho, "w", encoding="latin-1", newline="") as f:
        f.write(";".join(CAB) + "\n")
        for l in linhas:
            f.write(";".join(l) + "\n")
    return caminho


CADASTRO = [
    linha("JOSÉ ANDRÉ DA SILVA", "***.876.705-**", cargo="MINISTRO DE ESTADO"),
    linha("JOSÉ ANDRÉ DA SILVA", "***.876.705-**", sigla="CCX", nivel="0117", funcao="CARGO COMISSIONADO EXECUTIVO",
          ativ="SECRETARIO(A)", tipo="Função"),
    linha("MARIA SOUZA", "***.111.222-**", sigla="FEX", nivel="0113", funcao="FUNÇÃO COMISSIONADA EXECUTIVA", ativ="COORDENADOR(A)",
          tipo="Função"),
    linha("PEDRO LIMA", "***.333.444-**", cargo="ANALISTA", sit="APOSENTADO"),
    linha("PEDRO LIMA", "***.333.444-**", cargo="TECNICO", org="Ministério Y"),
]


class TestNormalizacao(unittest.TestCase):
    def test_nome_sem_acento_e_chave(self):
        self.assertEqual(sv.normalizar_nome("  José   André da Silva "), "JOSE ANDRE DA SILVA")
        self.assertEqual(sv.chave("José André da Silva", "***.876.705-**"), "JOSE ANDRE DA SILVA|***.876.705-**")

    def test_decodifica_funcao_em_cce_fce_com_das(self):
        d = sv.decodificar_funcao
        self.assertEqual(d("CCX-0115"), "CCE-1.15 (DAS 5)")
        self.assertEqual(d("CCX-0117"), "CCE-1.17 (DAS 6)")
        self.assertEqual(d("FEX-0113"), "FCE-1.13 (DAS 4)")
        self.assertEqual(d("CCX-0213"), "CCE-2.13 (DAS 4)")
        self.assertEqual(d("FG-0001"), "FG-0001")
        self.assertEqual(d("Sem informação"), "")
        self.assertEqual(d(""), "")


class TestCadastro(unittest.TestCase):
    def test_le_so_os_viajantes_pedidos_em_blocos(self):
        with tempfile.TemporaryDirectory() as p:
            cam = csv(p, CADASTRO)
            chaves = {sv.chave("José André da Silva", "***.876.705-**")}
            df = sv.ler_cadastro(cam, "siape", chaves, tamanho=2)
            self.assertEqual(len(df), 2)                      # as duas linhas dele; as outras pessoas ficaram de fora
            self.assertEqual(set(df["fonte"]), {"siape"})

    def test_consolida_cargo_e_funcao_de_linhas_diferentes(self):
        with tempfile.TemporaryDirectory() as p:
            cad = sv.ler_cadastro(csv(p, CADASTRO), "siape")
        c = sv.consolidar(cad, "202609").set_index("chave")
        jose = c.loc["JOSE ANDRE DA SILVA|***.876.705-**"]
        self.assertEqual((jose["cargo"], jose["funcao_codigo"], jose["atividade"]), ("MINISTRO DE ESTADO", "CCX-0117", "SECRETARIO(A)"))
        maria = c.loc["MARIA SOUZA|***.111.222-**"]
        self.assertEqual((maria["cargo"], maria["funcao_codigo"]), ("", "FEX-0113"))   # só função: cargo fica vazio
        pedro = c.loc["PEDRO LIMA|***.333.444-**"]
        self.assertEqual(pedro["lotacao"], "Ministério Y")      # vínculo ativo vence o aposentado

    def test_tabela_marca_nao_localizado(self):
        with tempfile.TemporaryDirectory() as p:
            cad = sv.ler_cadastro(csv(p, CADASTRO), "siape")
        viaj = pd.DataFrame({"cpf": ["***.876.705-**", "***.999.999-**"], "nome": ["JOSE ANDRE DA SILVA", "FULANO DE TAL"]})
        t = sv.montar_tabela(viaj, sv.consolidar(cad, "202609"), "202609").set_index("nome")
        self.assertEqual(t.loc["JOSE ANDRE DA SILVA", "status"], "localizado")
        self.assertEqual(t.loc["FULANO DE TAL", "status"], "nao_localizado")
        self.assertEqual(t.loc["FULANO DE TAL", "cargo"], "")

    def test_salvar_e_ler(self):
        t = pd.DataFrame([{c: "x" for c in sv.COLUNAS}])
        with tempfile.TemporaryDirectory() as p:
            sv.salvar(t, "202609", p)
            lido, meta = sv.ler(p)
            self.assertEqual((meta["referencia"], len(lido)), ("202609", 1))
            self.assertIsNone(sv.ler(tempfile.mkdtemp()))


class Resp:
    def __init__(self, status):
        self.status_code = status


class Sessao:
    def __init__(self, publicados):
        self.publicados = publicados

    def get(self, url, **kw):
        return Resp(206 if any(f"/{ym}_" in url for ym in self.publicados) else 403)


class TestMes(unittest.TestCase):
    def test_candidatos_do_mais_novo_para_o_mais_antigo(self):
        self.assertEqual(list(sv.meses_candidatos(dt.date(2026, 11, 15), 3)), ["202610", "202609", "202608"])
        self.assertEqual(list(sv.meses_candidatos(dt.date(2026, 1, 5), 2)), ["202512", "202511"])

    def test_pega_o_primeiro_mes_publicado(self):
        self.assertEqual(sv.mes_mais_recente(Sessao({"202609", "202608"}), dt.date(2026, 11, 15)), "202609")
        self.assertIsNone(sv.mes_mais_recente(Sessao(set()), dt.date(2026, 11, 15)))


class TestTela(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as p:
            cad = sv.ler_cadastro(csv(p, CADASTRO), "siape")
        viaj = pd.DataFrame({"cpf": ["***.876.705-**", "***.111.222-**"], "nome": ["JOSE ANDRE DA SILVA", "MARIA SOUZA"]})
        self.idx = sv.indice(sv.montar_tabela(viaj, sv.consolidar(cad, "202609"), "202609"))

    def test_localizado_mostra_cargo_atividade_e_funcao_legivel(self):
        i = tv.cargo_info("***.876.705-**", "JOSE ANDRE DA SILVA", "Sem informação", "CCX-0117", self.idx)
        self.assertEqual(i["cargo"], "MINISTRO DE ESTADO")
        self.assertEqual(i["funcao"], "CCE-1.17 (DAS 6)")
        self.assertEqual(i["origem"], "cadastro")

    def test_so_funcao_usa_a_atividade_como_cargo(self):
        i = tv.cargo_info("***.111.222-**", "MARIA SOUZA", "", "FEX-0113", self.idx)
        self.assertEqual(i["cargo"], "COORDENADOR(A)")
        self.assertEqual(i["funcao"], "FCE-1.13 (DAS 4)")

    def test_nao_localizado_cai_no_arquivo_de_viagens_ou_avisa(self):
        i = tv.cargo_info("***.000.000-**", "OUTRO", "PROFESSOR", "CCX-0113", self.idx)
        self.assertEqual((i["cargo"], i["origem"]), ("PROFESSOR", "viagem"))
        self.assertEqual(i["funcao"], "CCE-1.13 (DAS 4)")                  # a função da viagem também é decodificada
        j = tv.cargo_info("***.000.000-**", "OUTRO", "Sem informação", "Sem informação", self.idx)
        self.assertEqual(j["cargo"], tv.NAO_LOCALIZADO)
        k = tv.cargo_info("***.000.000-**", "OUTRO", "Sem informação", "Sem informação", {})   # sem cruzamento gerado
        self.assertEqual(k["cargo"], "")


class TestScript(unittest.TestCase):
    def test_viajantes_ignora_sigilo_e_cpf_sem_mascara(self):
        with tempfile.TemporaryDirectory() as p:
            pd.DataFrame({"cpf": ["***.1.2-**", "Informações protegidas por sigilo", "12345678901"],
                          "nome": ["A", "Informações protegidas por sigilo", "B"]}).to_csv(
                os.path.join(p, "viagens_2026_viajantes.csv"), index=False)
            v = script.viajantes(p)
            self.assertEqual(list(v["nome"]), ["A"])


if __name__ == "__main__":
    unittest.main()
