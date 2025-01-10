###############
# Purpose: Output metadata of the database
# The database information has already been configured in the config_lite.ini file.
###############
import sys,os
sys.path.insert(0, os.getcwd().lower())
from settings import z_config
from dbaccess.mysql.scmgen_m import ScmaGenerator as mMetaGen
from dbaccess.postgres.scmgen_p import ScmaGenerator as pMetaGen
from zebura_core.placeholder import make_dbServer
import zebura_core.constants as const
import asyncio

async def outMatadata():
    db_name = z_config['Training','db_name']
    server_name = z_config['Training','server_name']
    db_server = make_dbServer(server_name)
    db_server['db_name'] = db_name.lower()
    db_type = db_server['db_type']
    chat_lang = z_config['Training','chat_lang']

    if db_type == 'mysql':
        metaGen = mMetaGen(db_server, chat_lang)
    elif db_type == 'postgres':
        metaGen = pMetaGen(db_server, chat_lang)
    else:
        print(f"Unsupported database type: {db_type}")
        return False
    
    outPath = os.path.join(os.getcwd(),'training',db_name)
    if not os.path.exists(outPath):
        os.makedirs(outPath)
    xls_name = os.path.join(outPath, const.S_METADATA_FILE)

    xls_name = await metaGen.output_metadata(xls_name)
    await metaGen.summary_prompt(xls_name, 3000)

if __name__ == "__main__":
   asyncio.run(outMatadata())
    