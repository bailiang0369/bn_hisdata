# orderbook 目录的 pytest 配置。
#
# test_download.py 名字带 test_ 但不是单元测试：它是手动连通性脚本，
# 模块级代码会真去 api.cryptohftdata.com 下载一个小时文件（导入即执行）。
# 不排除的话 `pytest orderbook` / 仓库根 `pytest` 收集到它就会发起真实
# 网络请求（本机实测直接卡满 300s 读超时），并中断整个收集。
# 想手动跑它：python orderbook/test_download.py（不受影响）。
collect_ignore = ["test_download.py"]
