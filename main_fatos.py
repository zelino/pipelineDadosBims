import asyncio
from functions import BASE_URL, USER, PASSWORD, TENANT, login, insert_data, truncate_table, delete_records
from get_data import fetch_all_sales, fetch_all_products_sales,fetch_all_promotions_products, fetch_all_sales_void, fetch_all_promotions
from datetime import datetime, timedelta

async def process_sales_pipeline(session_id):
    all_sales_info = []
    all_found_ids = set()

    sales_params = {
        'status': 'all',
        'cost_values': 'taxed',
        'paid': 'all'
    }

    # Buscar dados de vendas
    sales_info, found_ids = await fetch_all_sales(session_id, BASE_URL, 'sales', sales_params)
    
    if not sales_info:
        # print("Nenhuma nova venda encontrada em ft_vendas")
        return
    
    all_sales_info.extend(sales_info)
    all_found_ids.update(found_ids)

    # Deletar registros existentes com IDs encontrados
    if all_found_ids:
        try:
            await delete_records('ft_vendas', 'venda_id', all_found_ids)
            print(f"Registros deletados com sucesso!")
        except Exception as e:
            print(f"Erro ao deletar registros: {str(e)}")

    # Inserir os novos dados
    try:
        await insert_data(all_sales_info, 'ft_vendas')
        # print(f"Dados inseridos com sucesso! {len(all_sales_info)}")
    except Exception as e:
        print(f"Erro ao inserir dados em ft_vendas: {str(e)}")

    if all_found_ids:
        print(f"IDs encontrados e processados: {len(all_found_ids)}")
        
async def process_sales_void_pipeline(session_id):
    all_sales_info = []
    all_found_ids = set()

    sales_params = {}

    sales_info, found_ids = await fetch_all_sales_void(session_id, BASE_URL, 'sales', sales_params)
    if not sales_info:
        print("Nenhuma nova venda encontrada em ft_vendas_canceladas")
        return

    all_sales_info.extend(sales_info)
    all_found_ids.update(found_ids)
    
    if all_sales_info:
        try:
            truncate_table('ft_vendas_canceladas')
            print (f'Trucate realizado')
        except Exception  as e:
            print(f'Erro ao fazer o truncate: {str(e)}')
        try:
            await insert_data(all_sales_info, 'ft_vendas_canceladas')
            print(f"Batch de {len(all_sales_info)} novos dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao inserir novos dados: {str(e)}")

    if all_found_ids:
        print(f"IDs processados: {len(all_found_ids)}")

    print("Processamento do pipeline de vendas canceladas concluído.")


async def process_products_sales_pipeline(session_id):
    all_sales_info = []
    all_found_ids = set()

    sales_params = {
       'status': 'all',
        'cost_values': 'taxed',
        'paid': 'all'
    }

    sales_info, found_ids = await fetch_all_products_sales(session_id, BASE_URL, 'sales', sales_params)
    if not sales_info:
        # print("Nenhuma nova venda encontrada em ft_vendas_detalhes")
        return
    
    all_sales_info.extend(sales_info)
    all_found_ids.update(found_ids)
        
    try:
        await delete_records('ft_vendas_detalhes', 'venda_id', all_found_ids)
        print("Registros deletados com sucesso")
    except Exception as e:
        print(f"Erro ao deletar registros: {str(e)}")
    try:
        await insert_data(all_sales_info, 'ft_vendas_detalhes')
    except Exception as e:
        print(f"Erro ao inserir dados em ft_vendas_detalhes: {str(e)}")
        
    if all_found_ids:
        print(f"IDs encontrados e processados: {len(all_found_ids)}")

async def process_promotions_pipeline(session_id):
    all_promotions_info = []
    all_found_ids = set()
    
    promotions_params = {}
    
    promotions_info, found_ids = await fetch_all_promotions(session_id, BASE_URL, 'promotions', promotions_params)
    
    all_promotions_info.extend(promotions_info)
    all_found_ids.update(found_ids)
    
    try:
        truncate_table('ft_promocoes')
        print("Tabela ft_promocoes truncada com sucesso!")
    except Exception as e:
        print(f"Erro ao truncar registros: {str(e)}")
    try:
        await insert_data(all_promotions_info, 'ft_promocoes')
    except Exception as e:
        print(f"Erro ao inserir dados em ft_promocoes: {str(e)}")
        
    if all_found_ids:
        print(f"IDs encontrados e processados: {len(all_found_ids)}")
        
        
async def process_products_promotions_pipeline(session_id):
    all_promotions_info = []
    all_found_ids = set()
    
    promotions_params = {}
    
    promotions_info, found_ids = await fetch_all_promotions_products(session_id, BASE_URL, 'promotions', promotions_params)
    
    all_promotions_info.extend(promotions_info)
    all_found_ids.update(found_ids)
    
    try:
        truncate_table('dim_promocoes_detalhes')
        print("Tabela dim_promocoes_detalhes truncada com sucesso!")
    except Exception as e:
        print(f"Erro ao truncar registros: {str(e)}")
    
    try:
        await insert_data(all_promotions_info, 'dim_promocoes_detalhes')
        print(f"Dados inseridos: {len(all_promotions_info)}")
    except Exception as e:
        print(f"Erro ao inserir dados em dim_promocoes_detalhes: {str(e)}")
        
    if all_found_ids:
        print(f"IDs encontrados e processados: {len(all_found_ids)}")

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
            await process_products_promotions_pipeline(session_id)
            await process_promotions_pipeline(session_id)
            # await process_products_sales_pipeline(session_id)
            # await process_sales_pipeline(session_id)
            # await process_sales_void_pipeline(session_id)
            print("Esperando proxima verificação em 5 minutos")
            await asyncio.sleep(300)
    else:
        print("Falha ao tentar fazer login")

if __name__ == "__main__":
    asyncio.run(main())
