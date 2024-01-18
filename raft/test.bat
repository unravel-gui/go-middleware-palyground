@echo off
setlocal enabledelayedexpansion

set maxAttempts=10
set currentAttempt=0
set testFailed=false

:runTest
if %currentAttempt% lss %maxAttempts% (
    echo Running go test attempt %currentAttempt%...
    go test .\part4\ >> error_log.txt

    rem 检查测试结果，如果失败则停止继续执行
    if errorlevel 1 (
        set testFailed=true
        echo Test failed on attempt %currentAttempt%. Exiting...
    ) else (
        echo Test passed on attempt %currentAttempt%.
        set /a currentAttempt+=1
        timeout /nobreak /t 1 >nul  rem 等待 1 秒
        goto runTest
    )
) else (
    echo Max attempts reached. Exiting...
)

if %testFailed%==true (
    echo Execution failed.
) else (
    echo All tests passed successfully.
)

endlocal
