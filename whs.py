from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, ID, TEXT
from whoosh.qparser import QueryParser
from whoosh.query import Or
import matplotlib.pyplot as plt
import time

caminho_documentos = ".\cran\cran.all.1400"
caminho_queries = ".\cran\cran.qry"
caminho_gabarito = ".\cran\cranqrel"
diretorio_index = ".\whoosh"

def indexar_documentos(caminho_arquivo, diretorio_index):
    schema = Schema(I=ID(stored=True), T=TEXT(stored=True), A=TEXT(stored=True), B=TEXT(stored=True), W=TEXT(stored=True))
    index = create_in(diretorio_index, schema)
    writer = index.writer()
    tempo_inicio = time.time()

    with open(caminho_arquivo, 'r') as arquivo:
        linhas = arquivo.readlines()
        id = 1
        documento_atual = {}
        tag_atual = None

        for linha in linhas:
            linha = linha.strip()
            if linha.startswith(".I"):
                if documento_atual:
                    writer.add_document(**documento_atual)
                    print(f"Indexed document {id}")
                    id += 1
                    documento_atual = {}
                documento_atual['I'] = linha.split()[-1]
            elif linha.startswith((".T", ".A", ".B", ".W")):
                tag_atual = linha.split()[0][1:]  #Remove o ponto da tag
                documento_atual[tag_atual] = ""
            elif tag_atual and linha:
                if tag_atual not in documento_atual:
                    documento_atual[tag_atual] = linha
                else:
                    documento_atual[tag_atual] += " " + linha

        if documento_atual:
            writer.add_document(**documento_atual)
            print(f"Indexado documento {id}")
    
    tempo_fim = time.time()
    tempo_percorrido = tempo_fim - tempo_inicio
    print(f"Tempo percorrido de indexação: {tempo_percorrido} segundos")

    writer.commit()



def encontrar_queries(arquivo_queries):
    with open(arquivo_queries, 'r') as arquivo:
        linhas = arquivo.readlines()
        queries = []
        query_atual = {}
        tag_atual = None
        buffer_counteudo = []
        numero_query = 1

        for linha in linhas:
            linha = linha.strip()
            if linha.startswith(".I"):
                if query_atual:
                    query_atual['W'] = " ".join(buffer_counteudo)
                    queries.append(query_atual)
                    query_atual = {}
                    buffer_counteudo = []
                query_atual['I'] = numero_query
                numero_query += 1
            elif query_atual and linha.startswith(".W"):
                tag_atual = ".W"
            elif query_atual and tag_atual == ".W" and linha:
                buffer_counteudo.append(linha)

        if query_atual:
            query_atual['W'] = " ".join(buffer_counteudo)
            queries.append(query_atual)
    return queries

def buscar(queries, diretorio_index, quantidade_resultados):
    index = open_dir(diretorio_index)
    documentos_encontrados = {}

    tempo_inicio_total = time.time()
    with index.searcher() as searcher:
        for query in queries:
            id = query['I']
            conteudo = query['W']
            documentos_coincidentes = []
            resultados_contador = 0
            
            tempo_inicio_busca = time.time() 
            
            # Cria um parser de consulta para o campo "W"
            qp = QueryParser("W", schema=index.schema)
            
            termos = conteudo.split()
            or_query = Or([qp.parse(termo) for termo in termos])
            
            # Realiza a busca com a consulta OR
            resultados = searcher.search(or_query, limit=quantidade_resultados)
            
            for resultado in resultados:
                if resultados_contador < quantidade_resultados:
                    documentos_coincidentes.append(resultado['I'])
                    resultados_contador += 1
            
            # Armazena os IDs dos documentos correspondentes (até max_results) para esta consulta no dicionário
            documentos_encontrados[id] = documentos_coincidentes
            
            tempo_fim_busca = time.time()
            tempo_busca = tempo_fim_busca - tempo_inicio_busca
            print(f"Busca para a consulta {id} levou: {tempo_busca} segundos")
    
    tempo_fim_total = time.time()
    tempo_total = tempo_fim_total - tempo_inicio_total 
    print(f"Tempo total de busca: {tempo_total} segundos")

    return documentos_encontrados

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

indexar_documentos(caminho_documentos, diretorio_index)
queries = encontrar_queries(caminho_queries)
documentos_encontrados = buscar(queries, diretorio_index, 100)
gabarito = documentos_relevantes(caminho_gabarito)
precision_at_k(documentos_encontrados, gabarito, 100)
recall_at_k(documentos_encontrados, gabarito, 100)
