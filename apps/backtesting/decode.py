import chardet

def decode_sql_file(file_path, convert_to_utf8=False, output_utf8_path=None):
    """
    解码 .sql 文件，自动尝试常见编码，可选转为 UTF-8
    
    参数：
    file_path: .sql 文件路径（必填）
    convert_to_utf8: 是否转为 UTF-8 编码（默认 False）
    output_utf8_path: 转换后 UTF-8 文件的保存路径（默认覆盖原文件）
    """
    # 常见编码列表（优先尝试中文场景高频编码，添加更多国际通用编码）
    common_encodings = [
        'utf-8', 'gbk', 'gb2312', 'ansi', 'latin-1', 
        'utf-8-sig', 'gb18030', 'cp1252',
        # 增加更多常用编码
        'utf-16', 'utf-16-le', 'utf-16-be', 'utf-32',
        'shift_jis', 'euc-jp', 'iso-2022-jp',  # 日文编码
        'euc-kr', 'iso-2022-kr', 'cp949',      # 韩文编码
        'big5', 'cp950', 'hz-gb-2312',         # 繁体中文编码
        'iso-8859-1', 'iso-8859-2', 'iso-8859-15', # ISO系列
        'cp1251', 'cp1250', 'cp1253', 'cp1254',    # Windows区域编码
        'mac-roman', 'cp437', 'cp850', 'cp852'     # 其他常见编码
    ]
    file_content = None
    success_encoding = None

    # 1. 尝试常见编码解码
    for encoding in common_encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                file_content = f.read()
            success_encoding = encoding
            print(f"✅ 解码成功！文件编码：{encoding}")
            # 解码成功时立即打印内容预览
            print("\n📄 解码后文件内容（前 500 字符）：")
            print("-" * 50)
            print(file_content[:500] + "..." if len(file_content) > 500 else file_content)
            print("-" * 50)
        except (UnicodeDecodeError, LookupError):
            print(f"❌ 编码 {encoding} 解码失败")
            continue

    # 2. 若常见编码失败，用 chardet 自动检测编码（兜底方案）
    if not success_encoding:
        print("🔍 常见编码尝试失败，自动检测编码...")
        with open(file_path, 'rb') as f:
            detect_result = chardet.detect(f.read())
        detect_encoding = detect_result['encoding']
        confidence = detect_result['confidence']
        if detect_encoding:
            try:
                with open(file_path, 'r', encoding=detect_encoding) as f:
                    file_content = f.read()
                success_encoding = detect_encoding
                print(f"✅ 自动检测解码成功！编码：{detect_encoding}（置信度：{confidence:.2f}）")
                # 自动检测编码成功时打印内容预览
                print("\n📄 解码后文件内容（前 500 字符）：")
                print("-" * 50)
                print(file_content[:500] + "..." if len(file_content) > 500 else file_content)
                print("-" * 50)
            except UnicodeDecodeError:
                print(f"❌ 自动检测的编码 {detect_encoding} 解码失败")
        else:
            print("❌ 所有编码尝试失败，文件可能损坏或编码异常")
            return None

    # 3. 注意：常见编码解码成功时已在上面打印内容预览
    # 如果是通过自动检测编码成功，则在这里打印内容

    # 4. 可选：转为 UTF-8 编码保存
    if convert_to_utf8 and success_encoding and file_content:
        output_path = output_utf8_path or file_path  # 默认覆盖原文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(file_content)
        print(f"\n✅ 已转为 UTF-8 编码并保存至：{output_path}")

    return file_content

# ------------------- 使用示例 -------------------
if __name__ == "__main__":
    # 替换为你的 .sql 文件路径
    SQL_FILE_PATH = "F:/work_codes/手动X-Ray/ATL-XRay-20251121/atl_xray.sql"  # 例如："C:/data/import.sql" 或 "./backup.sql"
    
    # 仅解码并查看内容（不修改原文件）
    decode_sql_file(SQL_FILE_PATH)
    
    # （可选）解码后转为 UTF-8 并保存（推荐，彻底解决乱码）
    # decode_sql_file(SQL_FILE_PATH, convert_to_utf8=True)
    
    # （可选）转为 UTF-8 并保存到新文件（不覆盖原文件）
    # decode_sql_file(SQL_FILE_PATH, convert_to_utf8=True, output_utf8_path="your_script_utf8.sql")