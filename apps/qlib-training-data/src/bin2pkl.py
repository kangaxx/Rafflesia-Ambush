import os
import pandas as pd
import pickle
import argparse
import importlib.util
import sys
from datetime import datetime

def load_config(config_file_path):
    """
    加载配置文件并提取所需参数
    
    Args:
        config_file_path (str): 配置文件的完整路径
        
    Returns:
        dict: 包含提取参数的字典
    """
    try:
        # 动态加载配置文件
        spec = importlib.util.spec_from_file_location("config", config_file_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        
        # 提取参数
        config_params = {}
        
        # 查找配置类实例
        config_instance = None
        
        # 查找可能的配置类实例名称
        possible_config_names = ['config', 'cfg', 'conf', 'settings']
        
        for attr_name in dir(config_module):
            if not attr_name.startswith('_'):
                attr_value = getattr(config_module, attr_name)
                # 检查是否是类实例且类名包含config
                if hasattr(attr_value, '__class__') and 'config' in attr_value.__class__.__name__.lower():
                    config_instance = attr_value
                    print(f"🔍 找到配置类实例: {attr_name} (类型: {attr_value.__class__.__name__})")
                    break
        
        # 如果没有找到明确的配置类实例，尝试查找Config类
        if config_instance is None:
            for attr_name in dir(config_module):
                if not attr_name.startswith('_') and 'config' in attr_name.lower():
                    attr_value = getattr(config_module, attr_name)
                    if hasattr(attr_value, '__class__') and hasattr(attr_value, 'dataset_path'):
                        config_instance = attr_value
                        print(f"🔍 找到配置类实例: {attr_name}")
                        break
        
        # 如果找到了配置类实例，从实例中提取参数
        if config_instance:
            print(f"📋 从配置类实例中提取参数...")
            
            # 获取 dataset_path (对应 output_dir)
            if hasattr(config_instance, 'dataset_path'):
                config_params['output_dir'] = config_instance.dataset_path
                print(f"   ✅ 找到 dataset_path: {config_instance.dataset_path}")
            
            # 获取时间范围参数
            # train_end 从 train_time_range 提取结束时间
            if hasattr(config_instance, 'train_time_range'):
                train_range = config_instance.train_time_range
                if isinstance(train_range, (list, tuple)) and len(train_range) >= 2:
                    config_params['train_end'] = train_range[1]
                    print(f"   ✅ 找到 train_time_range: {train_range} → train_end: {train_range[1]}")
            
            # val_end 从 val_time_range 提取结束时间
            if hasattr(config_instance, 'val_time_range'):
                val_range = config_instance.val_time_range
                if isinstance(val_range, (list, tuple)) and len(val_range) >= 2:
                    config_params['val_end'] = val_range[1]
                    print(f"   ✅ 找到 val_time_range: {val_range} → val_end: {val_range[1]}")
            
            # test_end 从 test_time_range 提取结束时间
            if hasattr(config_instance, 'test_time_range'):
                test_range = config_instance.test_time_range
                if isinstance(test_range, (list, tuple)) and len(test_range) >= 2:
                    config_params['test_end'] = test_range[1]
                    print(f"   ✅ 找到 test_time_range: {test_range} → test_end: {test_range[1]}")
        else:
            # 如果没有找到配置类实例，尝试从模块级别变量中提取
            print("🔍 未找到配置类实例，尝试从模块变量中提取...")
            
            # 获取 dataset_path (对应 output_dir)
            if hasattr(config_module, 'dataset_path'):
                config_params['output_dir'] = config_module.dataset_path
                print(f"   ✅ 找到 dataset_path: {config_module.dataset_path}")
            
            # 获取时间范围参数
            if hasattr(config_module, 'train_time_range'):
                train_range = config_module.train_time_range
                if isinstance(train_range, (list, tuple)) and len(train_range) >= 2:
                    config_params['train_end'] = train_range[1]
                    print(f"   ✅ 找到 train_time_range: {train_range} → train_end: {train_range[1]}")
            
            if hasattr(config_module, 'val_time_range'):
                val_range = config_module.val_time_range
                if isinstance(val_range, (list, tuple)) and len(val_range) >= 2:
                    config_params['val_end'] = val_range[1]
                    print(f"   ✅ 找到 val_time_range: {val_range} → val_end: {val_range[1]}")
            
            if hasattr(config_module, 'test_time_range'):
                test_range = config_module.test_time_range
                if isinstance(test_range, (list, tuple)) and len(test_range) >= 2:
                    config_params['test_end'] = test_range[1]
                    print(f"   ✅ 找到 test_time_range: {test_range} → test_end: {test_range[1]}")
        
        # 检查是否成功提取到任何参数
        if not config_params:
            print("⚠️  警告: 未从配置文件中提取到任何参数")
            # 打印可用的属性以供调试
            print("🔍 配置文件中的可用属性:")
            for attr_name in dir(config_module):
                if not attr_name.startswith('_'):
                    attr_value = getattr(config_module, attr_name)
                    if not callable(attr_value):  # 只显示非函数属性
                        print(f"   {attr_name}: {type(attr_value).__name__} = {attr_value}")
        else:
            print(f"\n✅ 从配置文件 {config_file_path} 成功加载 {len(config_params)} 个参数")
        
        return config_params
        
    except Exception as e:
        print(f"❌ 加载配置文件 {config_file_path} 失败: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
        return {}


def split_csv_to_pkl(
    csv_dir: str,
    output_dir: str,
    train_end: str,
    val_end: str,
    test_end: str = None,  # 新增：测试集结束日期
    date_col: str = "date",
    process_all: bool = False  # 新增：是否处理所有 CSV，默认 False（仅处理第一个）
) -> None:
    """
    将 CSV 目录下的文件按时间分割为 train/val/test .pkl
    默认仅处理第一个 CSV 文件，可通过 process_all=True 处理所有
    """
    # 校验日期格式
    try:
        train_end_dt = datetime.strptime(train_end, "%Y-%m-%d")
        val_end_dt = datetime.strptime(val_end, "%Y-%m-%d")
        
        # 如果提供了 test_end，则使用它，否则使用 val_end 之后的所有数据
        if test_end:
            test_end_dt = datetime.strptime(test_end, "%Y-%m-%d")
            if val_end_dt >= test_end_dt:
                raise ValueError("val_end 必须早于 test_end")
        else:
            test_end_dt = None
            
        if train_end_dt >= val_end_dt:
            raise ValueError("train_end 必须早于 val_end")
    except ValueError as e:
        raise ValueError(f"日期格式错误（需为 YYYY-MM-DD）：{e}")

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    train_path = os.path.join(output_dir, "train_data.pkl")
    val_path = os.path.join(output_dir, "val_data.pkl")
    test_path = os.path.join(output_dir, "test_data.pkl")

    # 初始化三个数据集（字典：key=股票代码，value=DataFrame）
    train_data = {}
    val_data = {}
    test_data = {}

    # 获取所有 CSV 文件并排序（确保第一个文件固定）
    csv_files = sorted([f for f in os.listdir(csv_dir) if f.endswith(".csv")])
    if not csv_files:
        raise FileNotFoundError(f"目录 {csv_dir} 中未找到 CSV 文件")

    # 决定处理的文件：默认第一个，process_all=True 则处理所有
    target_files = csv_files if process_all else [csv_files[0]]
    print(f"\n📁 文件处理信息:")
    print(f"   发现CSV文件数量: {len(csv_files)}")
    print(f"   处理模式: {'处理所有CSV文件' if process_all else '仅处理第一个CSV文件'}")
    print(f"   实际处理文件数量: {len(target_files)}")
    if not process_all and len(csv_files) > 1:
        print(f"   📝 提示: 使用 --process-all 参数可处理所有 {len(csv_files)} 个文件")
    print("-"*60)

    for idx, csv_file in enumerate(target_files, 1):
        csv_path = os.path.join(csv_dir, csv_file)
        instrument = os.path.splitext(csv_file)[0]  # 股票代码（文件名）

        try:
            # 读取 CSV（根据是否有日期表头处理索引）
            if date_col:
                df = pd.read_csv(csv_path, parse_dates=[date_col], index_col=date_col)
            else:
                df = pd.read_csv(csv_path, parse_dates=True, index_col=0)

            # 确保索引是 datetime 类型
            if not pd.api.types.is_datetime64_any_dtype(df.index):
                raise TypeError(f"{instrument} 的索引不是时间类型，请检查 CSV 格式")

            # 按时间分割数据
            train_df = df[df.index <= train_end_dt]
            val_df = df[(df.index > train_end_dt) & (df.index <= val_end_dt)]
            
            # 根据是否提供 test_end 分割测试集
            if test_end:
                test_df = df[(df.index > val_end_dt) & (df.index <= test_end_dt)]
            else:
                test_df = df[df.index > val_end_dt]

            # 过滤空数据集
            if not train_df.empty:
                train_data[instrument] = train_df
            if not val_df.empty:
                val_data[instrument] = val_df
            if not test_df.empty:
                test_data[instrument] = test_df

            print(f"[{idx}/{len(target_files)}] 处理完成：{instrument} "
                  f"(train: {len(train_df)}, val: {len(val_df)}, test: {len(test_df)})")

        except Exception as e:
            print(f"处理 {csv_file} 失败：{str(e)}")

    # 保存为 .pkl 文件
    with open(train_path, "wb") as f:
        pickle.dump(train_data, f)
    with open(val_path, "wb") as f:
        pickle.dump(val_data, f)
    with open(test_path, "wb") as f:
        pickle.dump(test_data, f)

    print(f"\n" + "="*60)
    print("✅ 数据处理完成")
    print("="*60)
    
    print(f"\n📊 数据集统计:")
    if test_end:
        print(f"   🟢 训练集（≤ {train_end}）: {len(train_data)} 只股票")
        print(f"   🟡 验证集（{train_end} < x ≤ {val_end}）: {len(val_data)} 只股票")
        print(f"   🔴 测试集（{val_end} < x ≤ {test_end}）: {len(test_data)} 只股票")
    else:
        print(f"   🟢 训练集（≤ {train_end}）: {len(train_data)} 只股票")
        print(f"   🟡 验证集（{train_end} < x ≤ {val_end}）: {len(val_data)} 只股票")
        print(f"   🔴 测试集（> {val_end}）: {len(test_data)} 只股票")
    
    print(f"\n💾 文件保存位置:")
    print(f"   训练集: {train_path}")
    print(f"   验证集: {val_path}")
    print(f"   测试集: {test_path}")
    
    print(f"\n🎯 处理结果:")
    total_stocks = len(train_data) + len(val_data) + len(test_data)
    print(f"   成功处理股票总数: {total_stocks}")
    print(f"   输出目录: {output_dir}")
    
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV 按时间分割为 train/val/test .pkl（默认处理第一个文件）")
    parser.add_argument("--csv-dir", type=str, required=True,
                        help="CSV 文件所在目录（如 ./qlib_merged_csv）")
    parser.add_argument("--output-dir", type=str, default="./split_pkl",
                        help="输出 train/val/test .pkl 的目录")
    parser.add_argument("--train-end", type=str, required=True,
                        help="训练集结束日期（如 2018-12-31）")
    parser.add_argument("--val-end", type=str, required=True,
                        help="验证集结束日期（如 2020-12-31）")
    parser.add_argument("--test-end", type=str, default=None,
                        help="测试集结束日期（如 2022-12-31），可选")
    parser.add_argument("--date-col", type=str, default="date",
                        help="CSV 中时间列的表头（若未指定表头则设为 ''）")
    parser.add_argument("--process-all", action="store_true",
                        help="添加此参数则处理所有 CSV 文件（默认仅处理第一个）")
    parser.add_argument("--config-file", type=str, default="/root/Kronos/finetune/config.py",
                        help="配置文件路径，用于读取参数（默认: /root/Kronos/finetune/config.py）")
    args = parser.parse_args()

    # 如果提供了配置文件，则从配置文件中读取参数
    config_params = {}
    if args.config_file and os.path.exists(args.config_file):
        config_params = load_config(args.config_file)
    
    # 使用配置文件中的参数覆盖命令行参数（如果存在）
    output_dir = config_params.get('output_dir', args.output_dir)
    train_end = config_params.get('train_end', args.train_end)
    val_end = config_params.get('val_end', args.val_end)
    test_end = config_params.get('test_end', args.test_end)
    
    # 验证必需参数
    if not train_end:
        raise ValueError("train_end 参数未提供，请通过命令行或配置文件设置")
    if not val_end:
        raise ValueError("val_end 参数未提供，请通过命令行或配置文件设置")

    print("\n" + "="*60)
    print("程序执行参数汇总")
    print("="*60)
    
    # 显示参数来源
    if config_params:
        print("📁 参数来源: 配置文件 + 命令行参数 (配置文件优先)")
        print(f"   配置文件路径: {args.config_file}")
    else:
        print("📁 参数来源: 命令行参数")
    
    print("\n📊 数据处理参数:")
    print(f"   CSV目录: {args.csv_dir}")
    print(f"   输出目录: {output_dir}")
    print(f"   日期字段: {args.date_col}")
    print(f"   处理模式: {'所有CSV文件' if args.process_all else '仅第一个CSV文件'}")
    
    print("\n📅 时间范围参数:")
    print(f"   训练集结束: {train_end}")
    print(f"   验证集结束: {val_end}")
    if test_end:
        print(f"   测试集结束: {test_end}")
        print(f"   时间范围: {train_end} → {val_end} → {test_end}")
    else:
        print(f"   测试集结束: 自动使用 {val_end} 之后的所有数据")
        print(f"   时间范围: {train_end} → {val_end} → 数据结束")
    
    print("\n🔍 参数详情:")
    print(f"   csv_dir: {args.csv_dir}")
    print(f"   output_dir: {output_dir}")
    print(f"   train_end: {train_end}")
    print(f"   val_end: {val_end}")
    print(f"   test_end: {test_end}")
    print(f"   date_col: {args.date_col}")
    print(f"   process_all: {args.process_all}")
    print(f"   config_file: {args.config_file}")
    
    print("="*60)
    print("开始处理数据...")
    print("-"*60)

    split_csv_to_pkl(
        csv_dir=args.csv_dir,
        output_dir=output_dir,
        train_end=train_end,
        val_end=val_end,
        test_end=test_end,
        date_col=args.date_col,
        process_all=args.process_all
    )