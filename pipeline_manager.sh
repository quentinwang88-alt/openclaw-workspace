#!/bin/bash
# Creator Grid Pipeline 快速启动脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$SCRIPT_DIR"
PIPELINE_SCRIPT="$WORKSPACE_DIR/creator_grid_pipeline.py"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# 检查 Python
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 未安装"
        exit 1
    fi
    print_info "Python 3: $(python3 --version)"
}

# 显示菜单
show_menu() {
    echo ""
    echo "=========================================="
    echo "  Creator Grid Pipeline 管理工具"
    echo "=========================================="
    echo ""
    echo "1. 运行流水线（处理所有待处理达人）"
    echo "2. 测试运行（处理 5 个达人）"
    echo "3. 查看状态"
    echo "4. 启动 HTTP 服务"
    echo "5. 查看日志"
    echo "6. 设置定时任务（crontab）"
    echo "7. 设置定时任务（launchd - macOS）"
    echo "8. 停止定时任务"
    echo "9. 退出"
    echo ""
}

# 运行流水线
run_pipeline() {
    print_info "开始运行流水线..."
    cd "$WORKSPACE_DIR" || exit 1
    python3 "$PIPELINE_SCRIPT" run
}

# 测试运行
test_run() {
    print_info "测试运行（处理 5 个达人）..."
    cd "$WORKSPACE_DIR" || exit 1
    python3 "$PIPELINE_SCRIPT" run --limit 5
}

# 查看状态
show_status() {
    print_info "查询流水线状态..."
    cd "$WORKSPACE_DIR" || exit 1
    python3 "$PIPELINE_SCRIPT" status
}

# 启动 HTTP 服务
start_http_service() {
    print_info "启动 HTTP 服务（端口 8766）..."
    print_info "按 Ctrl+C 停止服务"
    cd "$WORKSPACE_DIR" || exit 1
    python3 "$PIPELINE_SCRIPT" serve
}

# 查看日志
view_logs() {
    LOG_FILE="$WORKSPACE_DIR/output/pipeline.log"
    
    if [ ! -f "$LOG_FILE" ]; then
        print_warning "日志文件不存在: $LOG_FILE"
        return
    fi
    
    echo ""
    echo "1. 查看最近 50 行"
    echo "2. 实时查看日志"
    echo "3. 搜索错误"
    echo "4. 返回"
    echo ""
    read -p "请选择: " log_choice
    
    case $log_choice in
        1)
            tail -n 50 "$LOG_FILE"
            ;;
        2)
            print_info "实时查看日志（按 Ctrl+C 退出）..."
            tail -f "$LOG_FILE"
            ;;
        3)
            grep ERROR "$LOG_FILE"
            ;;
        4)
            return
            ;;
        *)
            print_error "无效选项"
            ;;
    esac
}

# 设置 crontab 定时任务
setup_crontab() {
    print_info "设置 crontab 定时任务"
    echo ""
    echo "选择运行频率："
    echo "1. 每天凌晨 2 点"
    echo "2. 每 6 小时"
    echo "3. 每周一早上 8 点"
    echo "4. 自定义"
    echo ""
    read -p "请选择: " freq_choice
    
    case $freq_choice in
        1)
            CRON_EXPR="0 2 * * *"
            ;;
        2)
            CRON_EXPR="0 */6 * * *"
            ;;
        3)
            CRON_EXPR="0 8 * * 1"
            ;;
        4)
            read -p "请输入 cron 表达式: " CRON_EXPR
            ;;
        *)
            print_error "无效选项"
            return
            ;;
    esac
    
    CRON_CMD="cd $WORKSPACE_DIR && /usr/bin/python3 $PIPELINE_SCRIPT run >> $WORKSPACE_DIR/output/pipeline_cron.log 2>&1"
    CRON_LINE="$CRON_EXPR $CRON_CMD"
    
    # 检查是否已存在
    if crontab -l 2>/dev/null | grep -q "creator_grid_pipeline.py"; then
        print_warning "定时任务已存在"
        read -p "是否覆盖？(y/n): " confirm
        if [ "$confirm" != "y" ]; then
            return
        fi
        # 删除旧任务
        crontab -l 2>/dev/null | grep -v "creator_grid_pipeline.py" | crontab -
    fi
    
    # 添加新任务
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    
    print_info "定时任务已设置"
    print_info "Cron 表达式: $CRON_EXPR"
    print_info "查看当前任务: crontab -l"
}

# 设置 launchd 定时任务（macOS）
setup_launchd() {
    if [ "$(uname)" != "Darwin" ]; then
        print_error "此功能仅支持 macOS"
        return
    fi
    
    PLIST_FILE="$HOME/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist"
    
    print_info "设置 launchd 定时任务"
    echo ""
    echo "选择运行频率："
    echo "1. 每天凌晨 2 点"
    echo "2. 每 6 小时"
    echo ""
    read -p "请选择: " freq_choice
    
    case $freq_choice in
        1)
            SCHEDULE_TYPE="calendar"
            SCHEDULE_VALUE="<key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>"
            ;;
        2)
            SCHEDULE_TYPE="interval"
            SCHEDULE_VALUE="<key>StartInterval</key>
    <integer>21600</integer>"
            ;;
        *)
            print_error "无效选项"
            return
            ;;
    esac
    
    # 创建 plist 文件
    cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.openclaw.creator-grid-pipeline</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>$PIPELINE_SCRIPT</string>
        <string>run</string>
    </array>
    
    <key>WorkingDirectory</key>
    <string>$WORKSPACE_DIR</string>
    
    <key>StandardOutPath</key>
    <string>$WORKSPACE_DIR/output/pipeline_launchd.log</string>
    
    <key>StandardErrorPath</key>
    <string>$WORKSPACE_DIR/output/pipeline_launchd_error.log</string>
    
    $SCHEDULE_VALUE
    
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
EOF
    
    # 加载任务
    launchctl unload "$PLIST_FILE" 2>/dev/null
    launchctl load "$PLIST_FILE"
    
    print_info "定时任务已设置"
    print_info "配置文件: $PLIST_FILE"
    print_info "查看状态: launchctl list | grep creator-grid-pipeline"
}

# 停止定时任务
stop_scheduled_task() {
    echo ""
    echo "1. 停止 crontab 任务"
    echo "2. 停止 launchd 任务（macOS）"
    echo "3. 返回"
    echo ""
    read -p "请选择: " stop_choice
    
    case $stop_choice in
        1)
            if crontab -l 2>/dev/null | grep -q "creator_grid_pipeline.py"; then
                crontab -l 2>/dev/null | grep -v "creator_grid_pipeline.py" | crontab -
                print_info "crontab 任务已停止"
            else
                print_warning "未找到 crontab 任务"
            fi
            ;;
        2)
            if [ "$(uname)" != "Darwin" ]; then
                print_error "此功能仅支持 macOS"
                return
            fi
            
            PLIST_FILE="$HOME/Library/LaunchAgents/com.openclaw.creator-grid-pipeline.plist"
            if [ -f "$PLIST_FILE" ]; then
                launchctl unload "$PLIST_FILE"
                rm "$PLIST_FILE"
                print_info "launchd 任务已停止"
            else
                print_warning "未找到 launchd 任务"
            fi
            ;;
        3)
            return
            ;;
        *)
            print_error "无效选项"
            ;;
    esac
}

# 主循环
main() {
    check_python
    
    while true; do
        show_menu
        read -p "请选择操作 (1-9): " choice
        
        case $choice in
            1)
                run_pipeline
                ;;
            2)
                test_run
                ;;
            3)
                show_status
                ;;
            4)
                start_http_service
                ;;
            5)
                view_logs
                ;;
            6)
                setup_crontab
                ;;
            7)
                setup_launchd
                ;;
            8)
                stop_scheduled_task
                ;;
            9)
                print_info "退出"
                exit 0
                ;;
            *)
                print_error "无效选项，请重新选择"
                ;;
        esac
        
        echo ""
        read -p "按 Enter 键继续..."
    done
}

# 运行主程序
main
