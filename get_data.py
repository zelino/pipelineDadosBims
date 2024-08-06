from functions import fetch_data
import aiohttp
import asyncio
import pandas as pd
import time
import json
from datetime import datetime, timedelta
#  (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
async def fetch_all_sales(session_id, base_url, endpoint, params_base, batch_size=500):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    offset = 0
    limit = batch_size
    dateTo =  datetime.now().strftime('%Y-%m-%d')
    dateFrom = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    # print(f"Buscando dados a partir de: {dateFrom} até: {dateTo}")

    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({
                'sid': session_id,
                'from': dateFrom,
                'to': dateTo,
                'limit': limit,
                'offset': offset
            })
            data = await fetch_data(session, url, params)
            
            if not data:
                # print("Nenhum dado encontrado para atualizar em ft_vendas.\n")
                break

            for item in data:
                sale = item['Sale']
                sale_info = {
                    'venda_id': int(sale['id']),
                    'data_hora': pd.to_datetime(sale['created']),
                    'data': pd.to_datetime(sale['issue_date']),
                    'valor_total': int(float(sale['amount']) or 0),
                    'valor_pago': int(float(sale['paid']) or 0),
                    'cod_empresa': int(sale['company_id'] or 0),
                    'agency_id': int(sale['agency_id'] or 0),
                    'cod_cliente': int(sale['contact_id'] or 0),
                    'status': str(sale['status']),
                    'void': bool(sale['void']),
                    'custo_venda': int(float(sale['cost']) or 0),
                    'regime_turismo': bool(sale['tourism_scheme']),
                    'cod_contact_country': int(sale['contact_country_id'] or 0),
                    'cod_cidade': int(sale['contact_country_id'] or 0),
                }
                data_info.append(sale_info)
                found_ids.add(int(sale['id']))

            # Atualizar o offset para a próxima página
            offset += limit
            # print(offset)
            await asyncio.sleep(0.1)  # Para evitar excesso de requisições
    
    return data_info, found_ids

async def fetch_all_products_sales(session_id, base_url, endpoint, params_base, batch_size=500):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    offset = 0
    limit = batch_size
    dateTo =  datetime.now().strftime('%Y-%m-%d')
    dateFrom = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    # print(f"Buscando dados a partir de: {dateFrom} até: {dateTo}")


    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({
                'sid': session_id,
                'from': dateFrom,
                'to': dateTo,
                'limit': limit,
                'offset': offset
            })
            data = await fetch_data(session, url, params)
            
            if not data:
                break

            for item in data:
                sales_products = item['SalesProduct']
                for product in sales_products:
                    product_info = {
                        # 'id': int(product.get('id', 0)),
                        'venda_id': int(product.get('sale_id', 0)),
                        'produto_id': int(product.get('product_id', 0)),
                        'quantidade': float(product.get('quantity', 0)),
                        'preco_un': float(product.get('price', 0)),
                        'custo_un': float(product.get('cost', 0)),
                        'taxa_desconto': float(product.get('discount_rate', 0)),
                        'desconto_total': float(product.get('discount_amount', 0)),
                        'tourism_scheme': bool(product.get('tourism_scheme', False)),
                        'pricing_id': int(product['pricing_id']) if product.get('pricing_id') is not None else 0,
                        'promocao_id': int(product.get('promotion_id', 0)) if product.get('promotion_id') is not None else 0
                    }
                    data_info.append(product_info)
                    found_ids.add(int(product.get('sale_id', 0)))
                    
            offset += limit
            # print (offset)
            await asyncio.sleep(0.1)
    return data_info, found_ids

async def fetch_all_products(session_id, base_url, endpoint, params_base, batch_size=500):
    url = f'{base_url}/{endpoint}'
    offset = 0
    limit = batch_size
    founds_ids = set()
    all_data_info = []

    # print(f"Iniciando busca com batch_size {batch_size}")

    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({'sid': session_id, 'limit': limit, 'offset': offset})
            
            data = await fetch_data(session, url, params)
            
            if not data:
                break
            
            data_info = []
            for item in data:
                product = item['Product']
                product_info = {
                    'produto_id': int(product['id']),
                    'nome': str(product['name']),
                    'empresa_id': int(product['company_id'] or 0),
                    'ativo_para_venda': bool(product['sellable']),
                    'ativo_para_compra': bool(product['buyable']),
                    'preco_venda': float(product['sell_price'] or 0),
                    'preco_custo': float(product['buy_price'] or 0),
                    'despesas_totais': float(product['total_expenses'] or 0),
                    'imagem': str(product['image']),
                    'referencia': str(product['code']),
                    'cod_barras': str(product['code2']),
                    'label_id': int(product['label_id'] or 0),
                    'qtd_estoque': float(product['availability'] or 0),
                    'ativo': bool(product['enabled']),
                    'cor': str(product['color']),
                    'provider_id': int(product['provider_id'] or 0),
                    'estoque_minimo': float(product['min_stock'] or 0),
                    'grupo_id': int(product['ptype_id'] or 0),
                }
                data_info.append(product_info)
                founds_ids.add(int(product['id']))
            
            if not data_info:
                # print("Nenhum dado novo encontrado.")
                break
            
            all_data_info.extend(data_info)
            offset += limit

    return all_data_info, founds_ids

async def fetch_all_providers(session_id, base_url, endpoint, params_base, batch_size=100):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    offset = 0
    limit = batch_size
    
    # print("Buscando por fornecedores")
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({
                'sid': session_id,
                'limit' : limit,
                'offset': offset
            })
            data = await fetch_data(session, url, params)
            
            if not data:
                break
            
            for item in data:
                provider = item['Contact']
                provider_info = {
                    'fornecedor_id': int(provider['id']),
                    'nome': str(provider['name']),
                    'nome_comercial': str(provider['comercial_name']),
                    'tipo': str(provider['type']),
                    'tipo_documento': str(provider['document_type']),
                    'documento': str(provider['document_id']),
                }
                data_info.append(provider_info)
                found_ids.add(int(provider['id']))
            offset += limit
            
            await asyncio.sleep(0.1)
            
    return data_info, found_ids

async def fetch_all_warehouses(session_id, base_url, endpoint, params_base):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    
    # print("Buscando por depositos")
    
    async with aiohttp.ClientSession() as session:
        params = params_base.copy()
        params.update({
            'sid': session_id
        })
        data = await fetch_data(session, url, params)
            
        if not data:
            # print("Nenhum dado encontrado em dim_depositos")
            return
            
        for item in data:
            warehouse = item['Warehouse']
            warehouse_info = {
                'deposito_id': int(warehouse['id']),
                'nome': str(warehouse['name']),
                'cod_empresa': int(warehouse['company_id'] or 0),
                'agency_id': int(warehouse['agency_id'] or 0),
            }
            data_info.append(warehouse_info)
            found_ids.add(int(warehouse['id']))
        
    return data_info, found_ids
            
async def fetch_all_agencies(session_id, base_url, endpoint, params_base):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    
    # print("Buscando por lojas")
    
    async with aiohttp.ClientSession() as session:
        params = params_base.copy()
        params.update({
            'sid': session_id,
        })
        data = await fetch_data(session, url, params)
            
        for item in data:
            agency = item['Agency']
            agency_info = {
                'loja_id': int(agency['id']),
                'nome': str(agency['name']),
                'cod_empresa': int(agency['company_id'] or 0),
                'endereco': str(agency['address']),
                'latitude': str(agency['lat']),
                'longitude': str(agency['lng']),
                'metragem': str(agency['phone'])
            }
            data_info.append(agency_info)
            found_ids.add(int(agency['id']))
                        
    return data_info, found_ids

async def fetch_all_stock(session_id, base_url, endpoint, params_base, batch_size=500):
    url = f'{base_url}/{endpoint}'
    offset = 0  # Início da paginação
    limit = batch_size  # Tamanho do lote
    data_info = []
    found_ids = set()
    
    # print(f"Iniciando com batch de {batch_size} para dim_estoque")
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({'sid': session_id, 'limit': limit, 'offset': offset})
            data = await fetch_data(session, url, params)

            if not data:
                # print(f"Nenhum dado encontrado.")
                break
            
            for item in data:
                product = item.get('Product', {})
                availabilities = product.get('AvailabilityFull', [])
                for estoque in availabilities:
                    estoque_info = {
                        'estoque_id': int(estoque.get('id', 0)),
                        'deposito_id': int(estoque.get('warehouse_id', 0)),
                        'produto_id': int(estoque.get('product_id', 0)),
                        'total': float(estoque.get('total', 0)),
                        'total2': float(estoque.get('total2', 0))
                    }
                    data_info.append(estoque_info)
                    found_ids.add(int(estoque.get('id', 0)))

            offset += limit
            await asyncio.sleep(0.1)
    
    return data_info, found_ids

async def fetch_all_ptypes(session_id, base_url, endpoint, params_base):
    url = f'{base_url}/{endpoint}'
    data_info = []
    found_ids = set()
    
    # print("Buscando grupos de produtos")
    
    async with aiohttp.ClientSession() as session:
        params = params_base.copy()
        params.update({'sid' : session_id})
        
        data = await fetch_data(session, url, params)
        
        if data:
            for item in data:
                ptype = item['Ptype']
                ptype_info = {
                    'grupo_id' : int(ptype['id']),
                    'cod_empresa' : int(ptype['company_id'] or 0),
                    'cod_loja' : int(ptype['agency_id'] or 0),
                    'nome' : str(ptype['name'])
                }
                data_info.append(ptype_info)
                found_ids.add(int(ptype['id']))
                
    return data_info, found_ids

async def fetch_all_promotions(session_id, base_url, endpoint, params_base, batch_size=150):
    url = f"{base_url}/{endpoint}"
    data_info = []
    found_ids = set()
    offset = 0
    limit = batch_size
    
    # print("Buscando por promoções")
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({
                'sid': session_id,
                'limit': limit,
                'offset': offset
            })
            data = await fetch_data(session, url, params)
            
            if not data:
                # print("Nenhum dado encontrado. Encerrando a busca.")
                break
            
            for item in data:
                promotion = item['Promotion']
                promotion_info = {
                    'promocao_id': int(promotion['id']),
                    'nome': str(promotion['name']),
                    'data_inicio': pd.to_datetime(promotion['start_date']),
                    'data_fim': pd.to_datetime(promotion['end_date']),
                    'ativo': bool(promotion['enabled']),
                    'taxa_desconto': float(promotion['discount_rate'] or 0)
                }
                data_info.append(promotion_info)
                found_ids.add(int(promotion['id']))
            
            if len(data) < limit:
                # print("Menos dados que o limite recebidos. Encerrando a busca.")
                break
            
            offset += limit
            # print(f"Offset atualizado para: {offset}")
            await asyncio.sleep(0.1)
    
    return data_info, found_ids


async def fetch_all_promotions_products(session_id, base_url, endpoint, params_base, batch_size=150):
    url = f"{base_url}/{endpoint}"
    data_info = []
    found_ids = set()
    offset = 0
    limit = batch_size
    
    # print("Buscando por produtos das promoções")
    
    async with aiohttp.ClientSession() as session:
        while True:
            params = params_base.copy()
            params.update({
                'sid': session_id,
                'limit': limit,
                'offset': offset
            })
            data = await fetch_data(session, url, params)
            
            if not data:
                # print("Nenhum dado encontrado. Encerrando a busca.")
                break
            
            print(f"Dados recebidos: {len(data)} itens.")
            
            for item in data:
                promotion_products = item.get('PromotionsProduct', [])
                
                for promotion in promotion_products:
                    promotion_info = {
                        'id': int(promotion.get('id', 0)),
                        'promocao_id': int(promotion.get('promotion_id', 0)),
                        'produto_id': int(promotion.get('product_id', 0)),
                        'qtd_minima': int(promotion.get('min_quantity', 0)),
                        'qtd_maxima': int(promotion.get('max_quantity', 0))
                    }
                    data_info.append(promotion_info)
                    found_ids.add(int(promotion.get('id', 0)))
            
            if len(data) < limit:
                # print("Menos dados que o limite recebidos. Encerrando a busca.")
                break
            
            offset += limit
            # print(f"Offset atualizado para: {offset}")
            await asyncio.sleep(0.1)
    
    return data_info, found_ids
