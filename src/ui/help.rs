use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::ui::theme;

pub fn render(frame: &mut Frame, area: Rect) {
    let help = r#"
 kafka-tui 快捷键帮助

 全局
   q        退出
   ?        帮助
   Ctrl+R   重载配置
   Alt+←/→  后退 / 前进
   c        打开 Cluster 下拉

 顶部工具栏
   ◀ ▶     后退 / 前进（浏览历史）
   Cluster ▼  切换 Cluster 连接

 鼠标
   单击     选中列表项 / 点击工具栏按钮 / 点击 Reply 重发
   双击     连接 Cluster / 进入 Topic
   滚轮     上下移动选择

 Cluster 选择
   j/k      上下移动
   Enter    连接
   c        打开 Cluster 下拉

 Topic 列表
   j/k      上下移动
   Enter    进入 Topic
   /        搜索
   r        刷新
   i        显示/隐藏内部 Topic
   c        切换 Cluster
   Esc      后退

 消息浏览
   j/k      上下移动
   Esc      返回 Topic 列表
   n/p      下一页/上一页
   点击 ◀▶  翻页（分页栏）
   点击 [50▼]  切换每页条数 (20/50/100)
   b/l      从头/从末尾
   g/t      跳转 offset/timestamp
   i        分区信息（每分区 offset 范围 / 条数）
   m        切换 Merged/Single 模式
   f        切换格式
   d        展开/折叠详情
   y/Y/k/C  复制 Value/Raw/Key/JSON
   R        Replay（可编辑）
   Reply    点击行末按钮预览并重发
   P        Produce

 Produce 弹窗
   Tab      切换字段
   Ctrl+S   发送
   Esc      取消

 确认弹窗 (Reply / 发送确认)
   ← →      切换 取消 / 确认
   Enter    执行当前选项
   Y / N    快捷确认 / 取消
   Esc      取消
"#;

    let widget = Paragraph::new(help)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" 帮助 ")
                .border_style(theme::MODAL_BORDER),
        )
        .wrap(Wrap { trim: true });

    frame.render_widget(widget, area);
}
