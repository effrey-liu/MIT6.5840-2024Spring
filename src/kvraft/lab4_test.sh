#!/bin/bash

# 初始化计数器
count=0
success_count=0
fail_count=0

# 设置测试次数
max_tests=50

rm -rf test_result4A.log

for ((i=1; i<=max_tests; i++))
do
    echo "Running test iteration $i of $max_tests..."
    
    # 运行 go 测试命令
    go test -run 4A &> test_result4A.log
    
    # 检查 go 命令的退出状态
    if [ "$?" -eq 0 ]; then
        # 测试成功
        success_count=$((success_count+1))
        echo "Test iteration $i passed."
    else
        # 测试失败
        fail_count=$((fail_count+1))
        echo "Test iteration $i failed, check 'failure4X.log' for details."
        mv test_result4A.log "failure4A_$i.log"
    fi
done

echo "Testing completed: $max_tests iterations run."
echo "Successes: $success_count"
echo "Failures: $fail_count"