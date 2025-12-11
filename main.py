"""
Prova Tecnica Nasajon – Integracao Supabase + IBGE + CSV

Autor: Guilherme Valim Araujo

Neste script eu implementei o fluxo completo pedido na prova:
- login no Supabase e obtencao do access_token (JWT)
- download da lista de municipios do IBGE
- normalizacao e matching dos nomes de municipios
- leitura do input.csv fornecido
- enriquecimento das linhas com dados oficiais do IBGE
- calculo de estatisticas
- envio das estatisticas para a Edge Function de correcao
"""

import csv
import json
import sys
import unicodedata
import difflib
import getpass
from collections import defaultdict
from typing import Dict, Any, List, Tuple

import requests


# aqui eu deixo todas as configuracoes externas como constantes
# pra ficar mais facil de mexer depois se precisar
SUPABASE_URL = "https://mynxlubykylncinttggu.supabase.co"

# chave publica do Supabase (anon key) que veio no enunciado
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im15bnhs"
    "dWJ5a3lsbmNpbnR0Z2d1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUxODg2NzAsImV4cCI6MjA4"
    "MDc2NDY3MH0.Z-zqiD6_tjnF2WLU167z7jT5NzZaG72dWH0dpQW1N-Y"
)

# endpoint publico do IBGE que devolve todos os municipios
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

# url da Edge Function que vai corrigir meu resultado
PROJECT_FUNCTION_URL = (
    "https://mynxlubykylncinttggu.functions.supabase.co/ibge-submit"
)

# nome do arquivo de entrada que a prova pediu
INPUT_CSV = "input.csv"

# nome do arquivo de saida que meu programa gera
OUTPUT_CSV = "resultado.csv"


def normalize_name(name: str) -> str:
    """
    aqui eu padronizo o nome dos municipios pra aumentar a chance de dar match:

    - removo acentos
    - converto pra minusculas
    - tiro simbolos estranhos, deixando so letras, numeros e espacos
    - removo espacos duplicados

    isso ajuda a tratar casos tipo:
    - "Sao Gonçalo" vs "Sao Goncalo"
    - "Curitba" vs "Curitiba"
    - erros simples de digitacao e formatacao
    """
    if not isinstance(name, str):
        return ""

    # separo letra e acento (ex.: "a" com til vira "a" + "~")
    nfkd = unicodedata.normalize("NFKD", name)

    # removo os caracteres que sao so acento
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))

    # jogo tudo pra minusculo pra evitar problema de maiuscula/minuscula
    lower = without_accents.lower()

    # aqui deixo so letras, numeros e espaco
    cleaned = []
    for c in lower:
        if c.isalnum() or c.isspace():
            cleaned.append(c)

    normalized = "".join(cleaned)

    # removo espacos duplicados
    return " ".join(normalized.split())


class IbgeIndex:
    """
    nessa classe eu crio um indice em memoria com todos os municipios do IBGE,
    pra fazer o matching rapido sem ficar chamando a API toda hora
    """

    def __init__(self, municipios: List[Dict[str, Any]]):
        # guardo a lista bruta de municipios que veio do IBGE
        self.municipios = municipios

        # dicionario que agrupa municipios pelo nome normalizado
        # exemplo: "santo andre" -> [municipio de SP, municipio de outro estado, etc]
        self.name_to_municipios: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # lista com todos os nomes normalizados, eu uso isso no fuzzy matching
        self.normalized_names: List[str] = []

        # aqui eu monto o indice
        for m in municipios:
            nome_oficial = m.get("nome", "")
            norm = normalize_name(nome_oficial)
            self.name_to_municipios[norm].append(m)
            self.normalized_names.append(norm)

        # removo nomes repetidos da lista
        self.normalized_names = list(set(self.normalized_names))

    def _choose_preferred_municipio(self, lista: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        quando existe mais de um municipio com o mesmo nome normalizado
        (tipo "santo andre"), eu preciso escolher um deles

        aqui eu usei uma logica simples:
        - tento priorizar municipios da regiao Sudeste
        - se nao achar nenhum do Sudeste, devolvo o primeiro da lista mesmo
        """
        for m in lista:
            try:
                mic = m.get("microrregiao", {}) or {}
                meso = mic.get("mesorregiao", {}) or {}
                uf_dict = meso.get("UF", {}) or {}
                regiao_dict = uf_dict.get("regiao", {}) or {}
                regiao_nome = regiao_dict.get("nome", "")
                if regiao_nome == "Sudeste":
                    return m
            except Exception:
                # se der erro na estrutura eu so ignoro e continuo
                continue

        # fallback se nao achou nada do sudeste
        return lista[0]

    def match(self, municipio_input: str) -> Tuple[str, Dict[str, Any]]:
        """
        faz o matching entre o nome que veio do CSV e o cadastro do IBGE

        retorno:
          - status: "OK" ou "NAO_ENCONTRADO"
          - muni_data: dicionario com dados do municipio quando status e "OK"
        """
        norm_input = normalize_name(municipio_input)

        # 1) tento match EXATO (nome normalizado igual)
        if norm_input in self.name_to_municipios:
            lista = self.name_to_municipios[norm_input]

            if len(lista) == 1:
                # so tem um municipio com esse nome -> match tranquilo
                return "OK", lista[0]
            else:
                # se tem mais de um municipio com o mesmo nome
                # em vez de marcar como ambiguo eu escolho um "preferido"
                escolhido = self._choose_preferred_municipio(lista)
                return "OK", escolhido

        # 2) tento match APROXIMADO (fuzzy) pra lidar com erro de digitacao
        candidates = difflib.get_close_matches(
            norm_input,
            self.normalized_names,
            n=3,
            cutoff=0.75,
        )

        if not candidates:
            # nada parecido o bastante -> marco como NAO_ENCONTRADO
            return "NAO_ENCONTRADO", {}

        best = candidates[0]
        lista = self.name_to_municipios[best]

        if len(lista) == 1:
            # fuzzy achou so um candidato, entao considero OK
            return "OK", lista[0]
        else:
            # fuzzy trouxe varios municipios diferentes
            # pra nao chutar errado eu prefiro marcar como NAO_ENCONTRADO
            return "NAO_ENCONTRADO", {}


def login_supabase(email: str, password: str) -> str:
    """
    aqui eu faco o login no Supabase usando email e senha

    se der certo eu recebo um access_token (JWT), que uso depois
    pra autorizar a chamada na Edge Function de correcao
    """
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"

    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Content-Type": "application/json",
    }

    payload = {
        "email": email,
        "password": password,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=20)

    if resp.status_code != 200:
        print("Erro ao fazer login no Supabase")
        print("Status:", resp.status_code)
        print("Corpo:", resp.text)
        raise SystemExit(1)

    data = resp.json()
    access_token = data.get("access_token")

    if not access_token:
        print("Nao consegui pegar o access_token na resposta do Supabase")
        print("Resposta:", data)
        raise SystemExit(1)

    print("Login no Supabase realizado com sucesso!")
    return access_token


def fetch_ibge_municipios() -> IbgeIndex:
    """
    baixa todos os municipios do IBGE de uma vez
    e devolve um IbgeIndex pra facilitar o matching depois
    """
    print("Baixando lista de municipios do IBGE...")

    resp = requests.get(IBGE_MUNICIPIOS_URL, timeout=60)

    if resp.status_code != 200:
        print("Erro ao chamar API do IBGE")
        print("Status:", resp.status_code)
        print("Corpo:", resp.text)
        raise SystemExit(1)

    municipios = resp.json()

    print(f"Total de municipios carregados do IBGE: {len(municipios)}")

    return IbgeIndex(municipios)


def processar_csv(ibge_index: IbgeIndex):
    """
    le o input.csv, tenta casar cada municipio com o IBGE,
    monta as linhas do resultado.csv e calcula as estatisticas da prova
    """
    linhas_output: List[Dict[str, Any]] = []

    total_municipios = 0
    total_ok = 0
    total_nao_encontrado = 0
    total_erro_api = 0
    pop_total_ok = 0

    # aqui eu somo as populacoes por regiao pra depois calcular a media
    soma_por_regiao = defaultdict(int)
    cont_por_regiao = defaultdict(int)

    with open(INPUT_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_municipios += 1

            municipio_input = row["municipio"]

            try:
                populacao_input = int(row["populacao"])
            except ValueError:
                # se a populacao vier zoada eu coloco 0 pra nao quebrar o programa
                populacao_input = 0

            status = "NAO_ENCONTRADO"
            municipio_ibge = ""
            uf = ""
            regiao = ""
            id_ibge = ""

            try:
                status_match, muni_data = ibge_index.match(municipio_input)
            except Exception:
                status_match = "ERRO_API"
                muni_data = {}

            status = status_match

            if status == "OK":
                try:
                    municipio_ibge = muni_data.get("nome", "")
                    id_ibge = muni_data.get("id", "")

                    mic = muni_data.get("microrregiao", {}) or {}
                    meso = mic.get("mesorregiao", {}) or {}
                    uf_dict = meso.get("UF", {}) or {}
                    regiao_dict = uf_dict.get("regiao", {}) or {}

                    uf = uf_dict.get("sigla", "")
                    regiao = regiao_dict.get("nome", "")

                    total_ok += 1
                    pop_total_ok += populacao_input

                    if regiao:
                        soma_por_regiao[regiao] += populacao_input
                        cont_por_regiao[regiao] += 1

                except Exception:
                    status = "ERRO_API"
                    total_erro_api += 1

            elif status == "NAO_ENCONTRADO":
                total_nao_encontrado += 1

            elif status == "ERRO_API":
                total_erro_api += 1

            linhas_output.append(
                {
                    "municipio_input": municipio_input,
                    "populacao_input": populacao_input,
                    "municipio_ibge": municipio_ibge,
                    "uf": uf,
                    "regiao": regiao,
                    "id_ibge": id_ibge,
                    "status": status,
                }
            )

    # aqui eu calculo as medias de populacao por regiao, so com status OK
    medias_por_regiao: Dict[str, float] = {}
    for regiao, soma in soma_por_regiao.items():
        qtd = cont_por_regiao[regiao]
        if qtd > 0:
            medias_por_regiao[regiao] = soma / qtd

    stats = {
        "total_municipios": total_municipios,
        "total_ok": total_ok,
        "total_nao_encontrado": total_nao_encontrado,
        "total_erro_api": total_erro_api,
        "pop_total_ok": pop_total_ok,
        "medias_por_regiao": medias_por_regiao,
    }

    return linhas_output, stats


def escrever_resultado_csv(linhas: List[Dict[str, Any]]):
    """
    gera o arquivo resultado.csv no formato que a prova pediu
    """
    campos = [
        "municipio_input",
        "populacao_input",
        "municipio_ibge",
        "uf",
        "regiao",
        "id_ibge",
        "status",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()

        for linha in linhas:
            writer.writerow(linha)

    print(f"Arquivo {OUTPUT_CSV} gerado com sucesso.")


def enviar_stats_para_edge_function(stats: Dict[str, Any], access_token: str) -> Dict[str, Any]:
    """
    envia as estatisticas pra Edge Function de correcao usando o access_token
    e devolve o json com score, feedback e detalhes
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {"stats": stats}

    resp = requests.post(
        PROJECT_FUNCTION_URL,
        headers=headers,
        json=payload,
        timeout=30,
    )

    if resp.status_code != 200:
        print("Erro ao enviar estatisticas para a API de correcao.")
        print("Status:", resp.status_code)
        print("Corpo:", resp.text)
        raise SystemExit(1)

    return resp.json()


def main():
    """
    funcao principal que junta todo o fluxo da prova:

    - pede email e senha
    - faz login no Supabase
    - baixa dados do IBGE
    - processa o input.csv
    - grava o resultado.csv
    - envia estatisticas pra correcao
    - mostra score e feedback na tela
    """
    print("=== Prova Tecnica Nasajon – IBGE / Supabase ===")

    email = input("Informe seu e-mail (Supabase): ")
    password = getpass.getpass("Informe sua senha: ")

    access_token = login_supabase(email, password)

    ibge_index = fetch_ibge_municipios()

    linhas_resultado, stats = processar_csv(ibge_index)

    escrever_resultado_csv(linhas_resultado)

    print("Enviando estatisticas para a API de correcao...")

    resposta = enviar_stats_para_edge_function(stats, access_token)

    print("\n=== Resposta da API de Correcao ===")
    print("user_id:", resposta.get("user_id"))
    print("email:", resposta.get("email"))
    print("score:", resposta.get("score"))
    print("feedback:", resposta.get("feedback"))

    components = resposta.get("components")
    if components:
        print("\nDetalhamento dos componentes:")
        print(json.dumps(components, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecucao interrompida pelo usuario.")
        sys.exit(0)
