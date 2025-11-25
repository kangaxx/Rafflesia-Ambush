import argparse

def main():
    weight_type_map = {
        "1": "amount",
        "2": "average",
        "3": "completely_average",
        "amount": "amount",
        "average": "average",
        "completely_average": "completely_average"
    }

    example_text = (
        "\n示例用法:\n"
        "  python create_index_contract.py "
        "-i .data "
        "-wt amount "
        "-o ./data/out/index_contract.csv "
        "-f RB\n"
        "\n参数默认值:\n"
        "  -i/--input: .data\n"
        "  -wt/--weight-type: amount\n"
        "  -o/--output: ./data/out/index_contract.csv\n"
        "  -f/--fut_code: RB\n"
    )

    parser = argparse.ArgumentParser(
        description="根据普通合约计算指数合约并保存结果。" + example_text,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-i", "--input", default=".data",
        help="普通合约文件的存储路径，默认为 .data。"
    )
    parser.add_argument(
        "-wt", "--weight-type", required=True, default="amount",
        choices=["1", "2", "3", "amount", "average", "completely_average"],
        help="权重计算类型：1 或 amount（持仓金额加权），2 或 average（单合约平均持仓金额），3 或 completely_average（完全平均）。默认 amount。"
    )
    parser.add_argument(
        "-o", "--output", required=True, default="./data/out/index_contract.csv",
        help="输出的指数合约文件路径，默认为 ./data/out/index_contract.csv。"
    )
    parser.add_argument(
        "-f", "--fut_code", required=True, default="RB",
        help="期货代码，默认为 RB。"
    )
    args = parser.parse_args()
    args.weight_type = weight_type_map[args.weight_type]
    # TODO: 实现指数合约计算与保存逻辑

if __name__ == "__main__":
    main()