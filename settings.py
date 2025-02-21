import configparser
import logging
import sys
import os

class Settings:
    isinstance_count = 0
    def __init__(self):
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(BASE_DIR, 'config_lite.ini'), encoding="utf-8")
        # self.config.read('config.ini',encoding='utf-8')
        self.settings()
        Settings.isinstance_count += 1
        print(f"Settings init success")

    def settings(self):
        # insert paths
        cwd = os.getcwd()
        module_path = self.config.get('Paths', 'ModulePath')
        sys.path.insert(0, cwd)
        sys.path.insert(0, os.path.join(cwd,module_path))

        # insert model path 所有子目录
        model_path = os.path.join(cwd,module_path)
        for root, dirs, files in os.walk(model_path):
            for dir in dirs:
                if dir[0] != '.' and dir[0] != '_':    
                    sys.path.insert(0, os.path.join(root, dir))
        # remove duplicates
        sys.path= list(set(path.lower() for path in sys.path))
        # logging level
        log_level_str = self.config.get('Logging', 'level')
        log_level = getattr(logging, log_level_str.upper(), logging.ERROR)
        #即保存到文件，又打印到屏幕
        logging.basicConfig(level=log_level, format='%(levelname)s - %(message)s',
                            handlers=[
                                logging.FileHandler("app.log"),
                                logging.StreamHandler()
                            ])
        # 打印日志级别
        log_level_name = logging.getLevelName(log_level)
        print(f'display log level: {log_level} - {log_level_name}')

            
    def __getitem__(self, keys):
        return self.config.get(keys[0], keys[1])
    
        
# Zebura settings
z_config = Settings()

# Example usage
if __name__ == '__main__':

    print("\n".join(sys.path))
    print(z_config['LLM','OPENAI_KEY'])
    message = "logging message"
    logging.debug(message)
    logging.info(message)
    logging.warning(message)
    logging.error(message)
    logging.critical(message)