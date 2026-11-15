import datetime as dt
import os
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import dou  # noqa: E402
import pncp  # noqa: E402
import sancoes  # noqa: E402


def api_item(inicio="16/07/2026", fim="15/07/2028", tipo="Impedimento/proibição de contratar"):
    return {"dataInicioSancao": inicio, "dataFimSancao": fim, "tipoSancao": {"descricaoResumida": tipo},
            "orgaoSancionador": {"nome": "PREFEITURA X", "siglaUf": "PR", "esfera": "MUNICIPAL"},
            "fundamentacao": [{"codigo": "LEI 14133 - ART. 156, III"}],
            "abrangenciaDefinidaDecisaoJudicial": "Só no ente", "linkPublicacao": ""}


class Resp:
    def __init__(self, status, payload=None):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p


class Sessao:
    def __init__(self, por_cadastro, status=200):
        self.por_cadastro, self.status, self.chamadas = por_cadastro, status, []

    def get(self, url, params=None, headers=None, **kw):
        cad = url.rsplit("/", 1)[1]
        self.chamadas.append((cad, params["codigoSancionado"], headers["chave-api-dados"]))
        if self.status != 200:
            return Resp(self.status)
        return Resp(200, self.por_cadastro.get(cad, []))


class TestConsulta(unittest.TestCase):
    def test_consulta_ceis_e_cnep_com_cnpj_so_digitos(self):
        s = Sessao({"ceis": [api_item()], "cnep": [api_item(tipo="Multa")]})
        r = sancoes.consultar_cnpj("37.121.913/0001-76", "K", s, pausa=0)
        self.assertEqual([x["cadastro"] for x in r], ["CEIS", "CNEP"])
        self.assertEqual(s.chamadas[0], ("ceis", "37121913000176", "K"))

    def test_sem_sancao_devolve_lista_vazia(self):
        self.assertEqual(sancoes.consultar_cnpj("00394460000141", "K", Sessao({}), pausa=0), [])

    def test_chave_recusada_e_erro_e_para(self):
        mapa, erros = sancoes.verificar(["11222333000144", "55666777000188"], "ruim", Sessao({}, status=401), pausa=0)
        self.assertEqual(mapa, {})
        self.assertEqual(len(erros), 1)  # parou no primeiro
        self.assertIn("chave", erros[0])

    @mock.patch("sancoes.time.sleep")
    def test_falha_vira_erro_nunca_sem_sancao(self, _):
        mapa, erros = sancoes.verificar(["11222333000144"], "K", Sessao({}, status=500), pausa=0)
        self.assertEqual(mapa, {})
        self.assertIn("HTTP 500", erros[0])

    def test_ignora_cpf_e_duplicados(self):
        s = Sessao({})
        mapa, _ = sancoes.verificar(["11222333000144", "11.222.333/0001-44", "12345678901"], "K", s, pausa=0)
        self.assertEqual(list(mapa), ["11222333000144"])


class TestStatus(unittest.TestCase):
    def test_status_na_data(self):
        d = dt.date(2026, 11, 13)
        f = sancoes.status_na_data
        self.assertEqual(f({"inicio": "16/07/2026", "fim": "15/07/2028"}, d), "vigente")
        self.assertEqual(f({"inicio": "16/07/2020", "fim": "15/07/2022"}, d), "encerrada")
        self.assertEqual(f({"inicio": "16/07/2027", "fim": "15/07/2028"}, d), "futura")
        self.assertEqual(f({"inicio": "16/07/2026", "fim": "Sem informação"}, d), "vigente")
        self.assertEqual(f({"inicio": "", "fim": ""}, d), "indeterminada")


class TestAplicar(unittest.TestCase):
    def contratos(self):
        from test_pncp import contrato
        return pncp.avaliar_contratos(pncp.normalizar_contratos([contrato()]))

    def test_sancao_vigente_sobe_para_alta(self):
        df = self.contratos()
        mapa = {"11222333000144": [sancoes._simplificar(api_item(), "ceis")]}
        r = sancoes.aplicar_sancoes(df, mapa).iloc[0]
        self.assertEqual(r["Score"], 50)
        self.assertIn("VIGENTE", r["Pontos de atenção"])
        self.assertIn("alcance", r["Pontos de atenção"])

    def test_sancao_encerrada_so_historico(self):
        df = self.contratos()
        mapa = {"11222333000144": [sancoes._simplificar(api_item("01/01/2020", "01/01/2022"), "ceis")]}
        r = sancoes.aplicar_sancoes(df, mapa).iloc[0]
        self.assertEqual(r["Score"], 10)
        self.assertIn("histórico", r["Pontos de atenção"])

    def test_sem_registro_nao_muda(self):
        df = self.contratos()
        r = sancoes.aplicar_sancoes(df, {"11222333000144": []}).iloc[0]
        self.assertEqual(r["Score"], 0)


if __name__ == "__main__":
    unittest.main()
