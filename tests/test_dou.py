import datetime as dt
import json
import os
import sys
import unittest
from unittest import mock

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dou  # noqa: E402
import notificar_fiscalizagov as notif  # noqa: E402

# Item no mesmo formato do jsonArray real do leiturajornal.
ITEM = {
    "pubName": "DO1",
    "urlTitle": "decisao-surod-n-1.875-de-5-de-novembro-de-2026-744141909",
    "numberPage": "199",
    "subTitulo": "",
    "titulo": "",
    "title": "DECISÃO SUROD Nº 1.875, DE 5 DE NOVEMBRO DE 2026",
    "pubDate": "13/11/2026",
    "content": "O Superintendente ... fica instituído o programa de benefício ...",
    "editionNumber": "214",
    "artType": "Decisão",
    "hierarchyStr": "Ministério dos Transportes/Agência Nacional de Transportes Terrestres",
}


def pagina(itens):
    payload = {"typeNormDay": "DO1", "dateUrl": "13-11-2026", "section": "DO1", "jsonArray": itens}
    return (
        "<html>\n\n<script id=\"params\" type=\"application/json\">\n"
        + json.dumps(payload) + "\n</script></html>"
    )


class RespostaFake:
    def __init__(self, status=200, text=""):
        self.status_code = status
        self.text = text


class SessaoFake:
    def __init__(self, respostas):
        self.respostas = list(respostas)
        self.chamadas = []

    def get(self, url, params=None, **kw):
        self.chamadas.append(params)
        r = self.respostas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


class TestParser(unittest.TestCase):
    def test_item_vira_schema_com_link_direto(self):
        itens = dou.normalizar_itens(dou.extrair_json_params(pagina([ITEM])), "do1")
        self.assertEqual(len(itens), 1)
        it = itens[0]
        self.assertEqual(it["Seção"], "DOU 1")
        self.assertEqual(it["Link"], "https://www.in.gov.br/web/dou/-/" + ITEM["urlTitle"])
        self.assertIn("Transportes", it["Órgão"])
        self.assertEqual(it["Página"], "199")

    def test_html_sem_bloco_params_e_erro(self):
        with self.assertRaises(dou.ErroColeta):
            dou.extrair_json_params("<html>acesso negado</html>")

    def test_json_sem_jsonarray_e_erro(self):
        with self.assertRaises(dou.ErroColeta):
            dou.extrair_json_params('<script id="params" type="application/json">{"a":1}</script>')


class TestColeta(unittest.TestCase):
    DATA = dt.date(2026, 11, 13)

    def test_data_vai_no_formato_dd_mm_aaaa(self):
        s = SessaoFake([RespostaFake(200, pagina([ITEM]))])
        df, erros = dou.coletar_dou(self.DATA, ["do1"], session=s)
        self.assertEqual(s.chamadas[0], {"data": "13-11-2026", "secao": "do1"})
        self.assertEqual((len(df), erros), (1, []))

    def test_dia_sem_publicacao_nao_e_erro(self):
        s = SessaoFake([RespostaFake(200, pagina([]))])
        df, erros = dou.coletar_dou(self.DATA, ["do1"], session=s)
        self.assertTrue(df.empty)
        self.assertEqual(erros, [])

    @mock.patch("dou.time.sleep")
    def test_falha_http_vira_erro_nunca_sem_achados(self, _):
        s = SessaoFake([RespostaFake(403)] * 3)
        df, erros = dou.coletar_dou(self.DATA, ["do1"], session=s)
        self.assertTrue(df.empty)
        self.assertEqual(len(erros), 1)
        self.assertIn("HTTP 403", erros[0])

    @mock.patch("dou.time.sleep")
    def test_retry_recupera_de_falha_transitoria(self, _):
        s = SessaoFake([requests.Timeout(), RespostaFake(200, pagina([ITEM]))])
        df, erros = dou.coletar_dou(self.DATA, ["do1"], session=s)
        self.assertEqual((len(df), erros), (1, []))

    @mock.patch("dou.time.sleep")
    def test_falha_parcial_mantem_o_que_veio_e_reporta(self, _):
        s = SessaoFake([RespostaFake(200, pagina([ITEM])), RespostaFake(500), RespostaFake(500), RespostaFake(500)])
        df, erros = dou.coletar_dou(self.DATA, ["do1", "do2"], session=s)
        self.assertEqual(len(df), 1)
        self.assertEqual(len(erros), 1)
        self.assertIn("DOU 2", erros[0])


class TestFiltroScore(unittest.TestCase):
    def setUp(self):
        self.df = dou.pd.DataFrame(dou.normalizar_itens(dou.extrair_json_params(pagina([ITEM])), "do1"))

    def test_filtrar(self):
        self.assertEqual(len(dou.filtrar(self.df, ["benefício"])), 1)
        self.assertEqual(len(dou.filtrar(self.df, ["inexistente"])), 0)

    def test_rankear_tem_score_e_motivos(self):
        r = dou.rankear(self.df, ["fica instituído"])
        self.assertGreater(r.iloc[0]["Score"], 10)
        self.assertIn("benefício", r.iloc[0]["Motivos"])


class TestNotificador(unittest.TestCase):
    ENV = {"TELEGRAM_BOT_TOKEN": "t", "TELEGRAM_CHAT_ID": "c", "FISCALIZAGOV_DATE": "2026-11-13"}

    def _rodar(self, coleta):
        enviados = []
        with mock.patch.dict(os.environ, self.ENV), \
                mock.patch("dou.coletar_dou", return_value=coleta), \
                mock.patch.object(notif, "telegram_send", side_effect=lambda t, c, m: enviados.append(m) or {"ok": True}):
            codigo = notif.main()
        return codigo, enviados

    def test_falha_de_coleta_avisa_e_retorna_1(self):
        codigo, msgs = self._rodar((dou.pd.DataFrame(columns=dou.COLUNAS), ["DOU 1: HTTP 403"]))
        self.assertEqual(codigo, 1)
        self.assertIn("FALHA NA COLETA", msgs[0])
        self.assertIn("HTTP 403", msgs[0])

    def test_dia_vazio_diz_que_nao_ha_publicacao(self):
        codigo, msgs = self._rodar((dou.pd.DataFrame(columns=dou.COLUNAS), []))
        self.assertEqual(codigo, 0)
        self.assertIn("nenhuma publicação", msgs[0])

    def test_achado_e_html_escapado(self):
        item = dict(ITEM, title="PORTARIA <script> & Cia", content="programa de benefício")
        df = dou.pd.DataFrame(dou.normalizar_itens(dou.extrair_json_params(pagina([item])), "do1"))
        codigo, msgs = self._rodar((df, []))
        self.assertEqual(codigo, 0)
        self.assertIn("&lt;script&gt; &amp; Cia", msgs[0])
        self.assertNotIn("<script>", msgs[0])
        self.assertIn("https://www.in.gov.br/web/dou/-/", msgs[0])


if __name__ == "__main__":
    unittest.main()
