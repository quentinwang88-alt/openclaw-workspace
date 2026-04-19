// Popup 界面脚本

document.addEventListener('DOMContentLoaded', async () => {
  await updateStatus();
  
  // 立即同步按钮
  document.getElementById('sync-now').addEventListener('click', async () => {
    const btn = document.getElementById('sync-now');
    btn.textContent = '同步中...';
    btn.disabled = true;
    
    try {
      const response = await chrome.runtime.sendMessage({ action: 'syncNow' });
      
      if (response.success) {
        showStatus('✅ 认证 Cookie 同步成功', 'active');
        setTimeout(() => updateStatus(), 1000);
      } else {
        // 更详细的错误信息
        let errorMsg = response.error || '未知错误';
        if (errorMsg.includes('Failed to fetch') || errorMsg.includes('NetworkError')) {
          errorMsg = '无法连接到本地服务器 (localhost:8765)。请先启动 token_receiver.py';
        }
        showStatus('❌ 同步失败: ' + errorMsg, 'error');
      }
    } catch (error) {
      showStatus('❌ 扩展错误: ' + error.message, 'error');
    }
    
    btn.textContent = '立即同步';
    btn.disabled = false;
  });
  
  // 检查服务器按钮
  document.getElementById('check-server').addEventListener('click', async () => {
    const btn = document.getElementById('check-server');
    btn.textContent = '检查中...';
    btn.disabled = true;
    
    try {
      const response = await fetch('http://localhost:8765/update-token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: 'test', timestamp: new Date().toISOString(), source: 'health-check' })
      });
      
      if (response.ok) {
        showStatus('✅ 服务器运行正常 (localhost:8765)', 'active');
      } else {
        showStatus('⚠️ 服务器响应异常: ' + response.status, 'inactive');
      }
    } catch (error) {
      showStatus('❌ 无法连接到服务器。请运行: ./start_token_receiver.sh', 'error');
    }
    
    btn.textContent = '检查服务器';
    btn.disabled = false;
  });
  
  // 切换监控按钮
  document.getElementById('toggle-monitoring').addEventListener('click', async () => {
    const response = await chrome.runtime.sendMessage({ action: 'toggleMonitoring' });
    await updateStatus();
  });
  
  // 打开配置按钮
  document.getElementById('open-settings').addEventListener('click', () => {
    chrome.tabs.create({ url: 'chrome://extensions/?id=' + chrome.runtime.id });
  });
});

async function updateStatus() {
  try {
    const status = await chrome.runtime.sendMessage({ action: 'getStatus' });
    
    const statusDiv = document.getElementById('status');
    const infoDiv = document.getElementById('info');
    
    if (status.isMonitoring && status.hasToken && status.hasJsessionid) {
      showStatus('🟢 监控中 - 认证已同步', 'active');
    } else if (status.isMonitoring && status.hasToken && !status.hasJsessionid) {
      showStatus('🟡 监控中 - 仅同步了 muc_token', 'inactive');
    } else if (status.isMonitoring && !status.hasToken) {
      showStatus('🟡 监控中 - 等待认证 Cookie', 'inactive');
    } else {
      showStatus('⚪ 监控已暂停', 'inactive');
    }
    
    // 更新详细信息
    document.getElementById('monitoring-status').textContent = 
      status.isMonitoring ? '运行中' : '已暂停';
    document.getElementById('token-status').textContent = 
      status.hasToken && status.hasJsessionid
        ? 'muc_token + JSESSIONID'
        : status.hasToken
          ? '仅 muc_token'
          : '未找到';
    document.getElementById('last-sync').textContent = 
      status.lastSync ? new Date(status.lastSync).toLocaleString('zh-CN') : '从未同步';
    
    infoDiv.style.display = 'block';
    
  } catch (error) {
    showStatus('❌ 无法连接到后台服务', 'error');
  }
}

function showStatus(message, type) {
  const statusDiv = document.getElementById('status');
  statusDiv.className = 'status ' + type;
  statusDiv.textContent = message;
}
