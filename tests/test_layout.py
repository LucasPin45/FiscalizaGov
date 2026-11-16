import os
import re
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import apresentacao  # noqa: E402
import ui  # noqa: E402

RAIZ = os.path.join(os.path.dirname(__file__), "..")


def rotulos_das_abas():
    src = open(os.path.join(RAIZ, "fiscalizagov.py"), encoding="utf-8").read()
    bloco = src.split("st.tabs([", 1)[1].split("])", 1)[0]
    return re.findall(r'"([^"]+)"', bloco)


class TestAbas(unittest.TestCase):
    def test_abas_do_dou_ficam_juntas_e_sem_telegram(self):
        r = rotulos_das_abas()
        self.assertEqual(r[0], "🏠 Apresentação")
        dou = [i for i, x in enumerate(r) if "DOU" in x or "Cargos" in x]
        self.assertEqual(dou, list(range(dou[0], dou[0] + len(dou))))      # posições consecutivas
        self.assertEqual(len(dou), 4)                                     # 3 abas do Diário + Cargos (atos do Diário)
        self.assertFalse(any("Telegram" in x for x in r))

    def test_css_do_grupo_dou_aponta_para_as_posicoes_6_a_9(self):
        r = rotulos_das_abas()
        dou = [i + 1 for i, x in enumerate(r) if "DOU" in x or "Cargos" in x]
        self.assertEqual((dou[0], dou[-1]), (6, 9))
        self.assertIn(":nth-child(n+6):nth-child(-n+9)", ui.CSS)


class TestComponentes(unittest.TestCase):
    def test_modulo_escapa_html_e_mostra_destino(self):
        h = ui.modulo("💸", "Gastos <b>", "R$ 1", "sub", ["linha <script>"], "Gastos do governo")
        self.assertNotIn("<script>", h)
        self.assertIn("&lt;script&gt;", h)
        self.assertIn("→ aba Gastos do governo", h)

    def test_modulo_indisponivel_nao_mostra_numeros(self):
        h = ui.modulo("🏗️", "Obras", "R$ 9", "sub", ["x"], "Obras e programas", indisponivel="Aguardando a primeira carga")
        self.assertIn("Aguardando a primeira carga", h)
        self.assertNotIn("R$ 9", h)

    def test_hero_tem_nome_e_chips(self):
        h = ui.hero("FiscalizaGov", "Painel", ["💸 Gastos", "📜 Diário Oficial"])
        self.assertIn("FiscalizaGov", h)
        self.assertEqual(h.count("fg-hero-chip"), 2)


class TestApresentacao(unittest.TestCase):
    def test_modulo_que_falha_vira_cartao_de_aviso_e_nao_quebra(self):
        def quebra():
            raise RuntimeError("fonte fora do ar")
        h = apresentacao._seguro(quebra, "💸", "Gastos do governo", "Gastos do governo", "#1b8a5a")
        self.assertIn("Não consegui ler esta fonte agora", h)
        self.assertIn("Gastos do governo", h)

    def test_cartoes_estaticos(self):
        h = apresentacao.card_dou({"atos": 2339, "palavras": 965, "altos": 3, "periodo": "13/11/2026"})
        self.assertIn("2.339 atos", h)
        self.assertIn("3 de relevância alta", h)
        self.assertIn("DOU · Achados", h)
        self.assertIn("Onde olhar primeiro", apresentacao.card_como_ler())

    def test_tema_verde_no_config(self):
        cfg = open(os.path.join(RAIZ, ".streamlit", "config.toml"), encoding="utf-8").read()
        self.assertIn('primaryColor = "#1b8a5a"', cfg)


if __name__ == "__main__":
    unittest.main()
