import os
import json

def find_file_path(file_name='RB9999.csv'):
    """
    查找指定文件名的路径
    首先检查当前目录下的./data目录
    如果不存在，则从default_param_list.json中读取tushare_root和index配置并拼接路径
    
    Args:
        file_name: 要查找的文件名
        
    Returns:
        str: 文件的完整路径，如果文件不存在则返回None
    """
    # 首先检查./Data目录（注意大小写）
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(current_dir, 'Data', file_name)
    
    if os.path.exists(data_dir_path):
        return data_dir_path
    
    # 如果不在./Data目录，则检查./data目录（小写）
    data_dir_lower = os.path.join(current_dir, 'data', file_name)
    if os.path.exists(data_dir_lower):
        return data_dir_lower
    
    # 读取default_param_list.json配置
    config_path = os.path.join(current_dir, 'default_param_list.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 拼接tushare_root和index路径
        tushare_root = config.get('tushare_root', '')
        index_path = config.get('index', '')
        
        # 展开波浪号
        tushare_root = os.path.expanduser(tushare_root)
        
        # 拼接完整路径
        full_path = os.path.join(tushare_root, index_path.lstrip('/'), file_name)
        
        if os.path.exists(full_path):
            return full_path
        else:
            return None
            
    except Exception as e:
        print(f"读取配置文件出错: {e}")
        return None

if __name__ == "__main__":
    # 测试查找RB9999.csv文件
    file_name = 'RB9999.csv'
    path = find_file_path(file_name)
    if path:
        print(f"成功找到文件 '{file_name}': {path}")
    else:
        print(f"未找到文件 '{file_name}'")
        
    # 额外输出当前目录信息用于调试
    print(f"当前工作目录: {os.getcwd()}")
    print(f"脚本所在目录: {os.path.dirname(os.path.abspath(__file__))}")
    print(f"./Data目录是否存在: {os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Data'))}")