import asyncio
from functions import BASE_URL, USER, PASSWORD, TENANT,login, truncate_table, insert_data, insert_data_in_batches
from get_data import fetch_all_products, fetch_all_providers, fetch_all_warehouses, fetch_all_agencies, fetch_all_stock, fetch_all_ptypes
from datetime import datetime, timedelta


async def process_products_pipeline(session_id):
    all_found_ids = set()
    params = {}
    product_info, found_ids = await fetch_all_products(session_id, BASE_URL, 'products', params)
    
    if not product_info:
        # print("Nenhum produto novo encontrado")
        return

    all_found_ids.update(found_ids)
    
    try:
        truncate_table('dim_produtos')
        # print("Tabela 'dim_produtos' truncada com sucesso")
        
        await insert_data(product_info, 'dim_produtos')
        # print("Dados inseridos com sucesso")
    except Exception as e:
        print(f"Erro ao processar dados: {str(e)}")
    
    if all_found_ids:
        print(f"Total de IDs processados em dim_produtos: {len(all_found_ids)}\n")
        
        
async def process_providers_pipeline(session_id):
    all_providers_info = []
    all_found_ids = set()
    
    providers_params = {
        'profile': 'provider'
    }
    
    provider_info, found_ids = await fetch_all_providers(session_id, BASE_URL, 'contacts', providers_params)
    
    if not provider_info:
        # print("Nenhum novo fornecedor encontrado")
        return

    all_providers_info.extend(provider_info)
    all_found_ids.update(found_ids)
    
    if all_providers_info:
        try:
            truncate_table('dim_fornecedores')
            # print("Tabela dim_fornecedores truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela dim_fornecedores: {str(e)}")
        
        try:
            await insert_data(all_providers_info, 'dim_fornecedores')
            # print(f"Batch de {len(all_providers_info)} novos dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir novos dados: {str(e)}")

    if all_found_ids:
        print(f"IDs processados em dim_fornecedores: {len(all_found_ids)}\n")
    
    # print("Processamento do pipeline de fornecedores concluído com sucesso")
    
    
async def process_warehouse_pipeline(session_id):
    all_warehouse_info = []
    all_found_ids = set()
    
    providers_params = {}
    
    warehouse_info, found_ids = await fetch_all_warehouses(session_id, BASE_URL, 'warehouses', providers_params)
    
    if not warehouse_info:
        # print("Nenhum novo deposito encontrado")
        return

    all_warehouse_info.extend(warehouse_info)
    all_found_ids.update(found_ids)
    
    if all_warehouse_info:
        try:
            truncate_table('dim_depositos')
            # print("Tabela dim_depositos truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela dim_depositos: {str(e)}")
        
        try:
            await insert_data(all_warehouse_info, 'dim_depositos')
            # print(f"Batch de {len(all_warehouse_info)} novos dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir novos dados: {str(e)}")

    if all_found_ids:
        print(f"IDs processados em dim_depositos: {len(all_found_ids)}\n")
    
    
async def process_agencies_pipeline(session_id):
    all_agency_info = []
    all_found_ids = set()
    
    providers_params = {}
    
    agency_info, found_ids = await fetch_all_agencies(session_id, BASE_URL, 'agencies', providers_params)
    
    if not agency_info:
        # print("Nenhuma nova loja encontrada")
        return

    all_agency_info.extend(agency_info)
    all_found_ids.update(found_ids)
    
    if all_agency_info:
        try:
            truncate_table('dim_lojas')
            # print("Tabela dim_lojas truncada com sucesso.")
        except Exception as e:
            print(f"Erro ao truncar a tabela dim_lojas: {str(e)}")
        
        try:
            await insert_data(all_agency_info, 'dim_lojas')
            # print(f"Batch de {len(all_agency_info)} novos dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir novos dados: {str(e)}")

    if all_found_ids:
        print(f"IDs processados em dim_lojas: {len(all_found_ids)}\n")
    
async def process_stock_pipeline(session_id):
    all_stock_info = []
    all_found_ids = set()
    stock_params = {}
    
    stock_info, found_ids = await fetch_all_stock(session_id, BASE_URL, 'products', stock_params)
    all_stock_info.extend(stock_info)
    all_found_ids.update(found_ids)
    
    if stock_info:
        try:
            truncate_table('dim_estoque')
            # print("Tabela dim_estoque truncada com sucesso!")
        except Exception as e:
            print(f"Erro ao truncar a tabela dim_estoque: {str(e)}")
        
        try:
            await insert_data_in_batches(all_stock_info, 'dim_estoque', batch_size=3000)
            # print(f"Dados inseridos {len(all_stock_info)}")
        except Exception as e:
            print(f"Erro ao inserir novos dados: {str(e)}")
    if all_found_ids:
        print(f"IDs processados em dim_estoque: {len(all_found_ids)}\n")
        
    
async def process_ptype_pipeline(session_id):
    all_ptype_info = []
    all_found_ids = set()
    
    ptypes_params = {}
    
    ptype_info, found_ids = await fetch_all_ptypes(session_id, BASE_URL, 'ptypes', ptypes_params)
    
    if not ptype_info:
        # print("Nenhum novo grupo encontrado")
        return
    
    all_ptype_info.extend(ptype_info)
    all_found_ids.update(found_ids)
    
    if all_ptype_info:
        try:
            truncate_table('dim_grupos')
            # print("Tabela dim_grupos truncado com sucesso!")
        except Exception as e:
            print(f"Erro ao truncar tabela dim_grupos: {str(e)}")
        
        try:
            await insert_data(all_ptype_info, 'dim_grupos')
            # print(f"Batch de {len(all_ptype_info)} novos dados inseridos com sucesso")
        except Exception as e:
            print(f"Erro ao inserir novos dados em dim_grupos: {str(e)}\n")
            
    if all_found_ids:
        print(f"IDs processados {len(all_found_ids)}")
    
async def main():
    session_id = await login(BASE_URL, USER, PASSWORD, TENANT)
    if session_id:
        print("Logado!")
        last_login_time = datetime.now()
        while True:   
            current_time = datetime.now()
            elapsed_time = current_time - last_login_time
            if elapsed_time > timedelta(hours=23):
                print("Sessao expirada, realizando novo login")
                session_id = await login(BASE_URL, USER, PASSWORD, TENANT)
                if session_id:
                    print("Logado novamente")
                    last_login_time = current_time
                else:
                    print("Falha ao tentar logar, tentando login novamente em 60 segundos")
                    await asyncio.sleep(60)
                    continue
            await process_ptype_pipeline(session_id)
            await process_agencies_pipeline(session_id)
            await process_warehouse_pipeline(session_id)
            await process_providers_pipeline(session_id)
            await process_products_pipeline(session_id)
            await process_stock_pipeline(session_id)
            print("Esperando proxima verificação em 2 horas")
            await asyncio.sleep(7200)
    else:
        print("Falha ao tentar fazer login")

if __name__ == "__main__":
    asyncio.run(main())

