from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
import time
import concurrent.futures


ELASTIC_PASSWORD = ""
CLOUD_ID = ""

client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)

caminho_documentos = ".\cran\cran.all.1400"
caminho_queries = ".\cran\cran.qry"
caminho_gabarito = ".\cran\cranqrel"

def indexar_documentos(caminho_arquivo):
    with open(caminho_arquivo, 'r') as arquivo:
            linhas = arquivo.readlines()
            nome_indice = "documentos"
            id = 1
            documento_atual = {}
            tag_atual = None
            tempo_inicio = time.time()
            documentos_para_indexar = []

            for linha in linhas:
                linha = linha.strip()
                if linha.startswith(".I"):
                    if documento_atual:
                        documentos_para_indexar.append((id, documento_atual.copy()))
                        id += 1
                        documento_atual = {}
                    documento_atual['I'] = linha.split()[-1]
                elif linha.startswith((".T", ".A", ".B", ".W")):
                    tag_atual = linha.split()[0][1:]
                    documento_atual[tag_atual] = ""
                elif tag_atual and linha:
                    if tag_atual not in documento_atual:
                        documento_atual[tag_atual] = linha
                    else:
                        documento_atual[tag_atual] += " " + linha

            if documento_atual:
                documentos_para_indexar.append((id, documento_atual))

            def indexar_documento(doc):
                id, documento = doc
                resposta = client.index(index=nome_indice, id=id, body=documento)
                print(f"Documento {id} indexado: {resposta['result']}")

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(indexar_documento, documentos_para_indexar)

            tempo_fim = time.time()
            tempo_percorrido = tempo_fim - tempo_inicio
            print(f"Tempo percorrido de indexação: {tempo_percorrido} segundos")

def documentos_relevantes(caminho_arquivo):
    documentos = {}

    with open(caminho_arquivo, 'r') as arquivo:
        linhas = arquivo.readlines()

    for linha in linhas:
        colunas = linha.strip().split()
        if len(colunas) == 3:
            primeira_coluna = colunas[0]
            segunda_coluna = colunas[1]

            if primeira_coluna not in documentos:
                documentos[primeira_coluna] = []

            documentos[primeira_coluna].append(segunda_coluna)

    return documentos

def encontrar_queries(queries_arquivo):
    with open(queries_arquivo, 'r') as arquivo:
        linhas = arquivo.readlines()
        queries = []
        query_atual = {}
        tag_atual = None
        buffer_conteudo = []
        numero_query = 1 

        for linha in linhas:
            linha = linha.strip()
            if linha.startswith(".I"):
                if query_atual:
                    query_atual['W'] = " ".join(buffer_conteudo)
                    queries.append(query_atual)
                    query_atual = {}
                    buffer_conteudo = []
                query_atual['I'] = numero_query
                numero_query += 1
            elif query_atual and linha.startswith(".W"):
                tag_atual = ".W"
            elif query_atual and tag_atual == ".W" and linha:
                buffer_conteudo.append(linha)

        if query_atual:
            query_atual['W'] = " ".join(buffer_conteudo)
            queries.append(query_atual)
    return queries

def buscar(queries, quantidade):
    documentos_encontrados = {query.get('I'): [] for query in queries}

    def buscar_documento(query):
        id = query['I']
        conteudo = query['W']

        query_busca = {
            "size": quantidade,
            "query": {
                "match": {
                    "W": conteudo
                }
            }
        }

        tempo_inicio_busca = time.time()  

        resultados = client.search(index="documentos", body=query_busca)

        tempo_fim_busca = time.time()
        tempo_busca = tempo_fim_busca - tempo_inicio_busca
        print(f"Busca para a consulta {id} levou {tempo_busca} segundos")

        documentos_encontrados[id].extend(hit['_id'] for hit in resultados['hits']['hits'])


    tempo_inicio = time.time()

    #ThreadPoolExecutor realiza buscas em paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(buscar_documento, queries)

    tempo_fim = time.time()
    tempo_percorrido = tempo_fim - tempo_inicio
    print(f"Tempo percorrido de busca: {tempo_percorrido} segundos")

    return documentos_encontrados


def precision_at_k(documentos_encontrados, documentos_relevantes, k_maximo):
    k_valores = range(1, k_maximo+1)
    precisao_valores = []

    for k in k_valores:
        soma_precisao = 0
        for id, relevant_docs in documentos_relevantes.items():
            documentos_encontrados_query = documentos_encontrados.get(int(id), [])
            k_documentos_encontrados = documentos_encontrados_query[:k]
            documentos_relevantes_set = set(relevant_docs)
            k_relevantes = [doc for doc in k_documentos_encontrados if doc in documentos_relevantes_set]
            precisao = len(k_relevantes) / k if k != 0 else 0
            soma_precisao += precisao
        media_aritmetica_k = soma_precisao / len(documentos_relevantes)
        precisao_valores.append(media_aritmetica_k)

    #Plot do gráfico
    plt.figure(figsize=(8, 6))
    plt.plot(k_valores, precisao_valores, marker='o', linestyle='-')
    plt.xlabel('k')
    plt.ylabel('Precision@k (Média)')
    plt.title('Precision@k Gráfico')
    plt.grid(True)
    plt.xticks(k_valores)
    plt.show()

def recall_at_k(documentos_encontrados, documentos_relevantes, k_maximo):
    k_valores = range(1, k_maximo+1)
    recall_valores = []

    for k in k_valores:
        recall_soma = 0
        for query_id, documentos in documentos_relevantes.items():
            documentos_encontrados_query = documentos_encontrados.get(int(query_id), [])
            documentos_encontrados_k = documentos_encontrados_query[:k]
            documentos_relevantes_set = set(documentos)
            relevante_k = [doc for doc in documentos_encontrados_k if doc in documentos_relevantes_set]
            recall = len(relevante_k) / len(documentos) if len(documentos) != 0 else 0
            recall_soma += recall
        media_recall_k = recall_soma / len(documentos_relevantes)
        recall_valores.append(media_recall_k)

    #Plot do gráfico
    plt.figure(figsize=(8, 6))
    plt.plot(k_valores, recall_valores, marker='o', linestyle='-')
    plt.xlabel('k')
    plt.ylabel('Recall@k (Média)')
    plt.title('Recall@k Gráfico')
    plt.grid(True)
    plt.xticks(k_valores)
    plt.show()


indexar_documentos(caminho_documentos)
queries = encontrar_queries(caminho_queries)
gabarito = documentos_relevantes(caminho_gabarito)
documentos_encontrados = buscar(queries, 100)
precision_at_k(documentos_encontrados, gabarito, 100)
recall_at_k(documentos_encontrados, gabarito, 100)
