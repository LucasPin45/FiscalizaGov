import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from segredos import chave_transparencia  # noqa: E402


class TestChave(unittest.TestCase):
    def test_na_raiz(self):
        self.assertEqual(chave_transparencia({"TRANSPARENCIA_API_KEY": "abc"}, {}), "abc")

    def test_dentro_de_auth_como_no_print_do_usuario(self):
        self.assertEqual(chave_transparencia({"auth": {"senha": "x", "TRANSPARENCIA_API_KEY": " abc "}}, {}), "abc")

    def test_variavel_de_ambiente(self):
        self.assertEqual(chave_transparencia({}, {"TRANSPARENCIA_API_KEY": "env"}), "env")

    def test_raiz_tem_prioridade_e_ausente_e_vazio(self):
        self.assertEqual(chave_transparencia({"TRANSPARENCIA_API_KEY": "r", "auth": {"TRANSPARENCIA_API_KEY": "a"}}, {}), "r")
        self.assertEqual(chave_transparencia({}, {}), "")

    def test_secrets_quebrado_nao_levanta(self):
        class Quebrado:
            def __getitem__(self, k):
                raise FileNotFoundError("sem secrets.toml")
        self.assertEqual(chave_transparencia(Quebrado(), {}), "")


if __name__ == "__main__":
    unittest.main()
