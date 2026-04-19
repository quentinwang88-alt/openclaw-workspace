// BigSeller Token 自动同步后台脚本

const CONFIG = {
  domain: 'bigseller.pro',
  monitoredCookieNames: ['muc_token', 'JSESSIONID', 'language', 'fingerPrint'],
  localServerUrl: 'http://localhost:8765/update-token',
  checkInterval: 5000, // 5秒检查一次
};

let lastCookieSignature = null;
let lastSyncAt = null;
let monitorIntervalId = null;
let isMonitoring = false;

function buildCookieString(cookieMap) {
  const parts = [];
  const seen = new Set();

  for (const name of CONFIG.monitoredCookieNames) {
    if (cookieMap[name]) {
      parts.push(`${name}=${cookieMap[name]}`);
      seen.add(name);
    }
  }

  for (const [name, value] of Object.entries(cookieMap)) {
    if (!seen.has(name) && value) {
      parts.push(`${name}=${value}`);
    }
  }

  return parts.length > 0 ? `${parts.join('; ')};` : '';
}

function buildCookieSignature(cookieMap) {
  return CONFIG.monitoredCookieNames
    .map((name) => `${name}=${cookieMap[name] || ''}`)
    .join('|');
}

// 获取当前认证 Cookie
async function getCurrentAuthSnapshot() {
  try {
    const cookies = await chrome.cookies.getAll({ domain: CONFIG.domain });
    if (cookies.length === 0) {
      return null;
    }

    const cookieMap = {};
    for (const cookie of cookies) {
      if (cookie.value) {
        cookieMap[cookie.name] = cookie.value;
      }
    }

    if (Object.keys(cookieMap).length === 0) {
      return null;
    }

    return {
      cookies: cookieMap,
      cookie: buildCookieString(cookieMap),
      token: cookieMap.muc_token || null,
      jsessionid: cookieMap.JSESSIONID || null,
      signature: buildCookieSignature(cookieMap),
    };
  } catch (error) {
    console.error('获取认证 Cookie 失败:', error);
    return null;
  }
}

// 发送认证 Cookie 到本地服务器
async function sendTokenToServer(authSnapshot) {
  try {
    const response = await fetch(CONFIG.localServerUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        token: authSnapshot.token,
        jsessionid: authSnapshot.jsessionid,
        cookie: authSnapshot.cookie,
        cookies: authSnapshot.cookies,
        timestamp: new Date().toISOString(),
        source: 'browser-extension'
      })
    });
    
    if (response.ok) {
      const result = await response.json();
      console.log('✅ Token 同步成功:', result);
      return { success: true, result };
    } else {
      const errorText = await response.text();
      let errorMessage = `服务器返回错误 ${response.status}`;

      try {
        const errorJson = JSON.parse(errorText);
        if (errorJson.error) {
          errorMessage = errorJson.error;
        }
      } catch (error) {
        if (errorText) {
          errorMessage = `${errorMessage}: ${errorText}`;
        }
      }

      console.error('❌ Token 同步失败:', response.status, errorText);
      return { success: false, error: errorMessage };
    }
  } catch (error) {
    console.error('❌ 无法连接到本地服务器:', error.message);
    return {
      success: false,
      error: `无法连接到本地服务器 (${CONFIG.localServerUrl})。请确保 token_receiver.py 正在运行。错误: ${error.message}`
    };
  }
}

// 监听认证 Cookie 变化
async function monitorToken() {
  const authSnapshot = await getCurrentAuthSnapshot();

  if (authSnapshot && authSnapshot.signature !== lastCookieSignature) {
    console.log('🔄 检测到认证 Cookie 变化');
    const result = await sendTokenToServer(authSnapshot);

    if (result.success) {
      lastCookieSignature = authSnapshot.signature;
      lastSyncAt = new Date().toISOString();
      // 更新扩展图标状态
      chrome.action.setBadgeText({ text: '✓' });
      chrome.action.setBadgeBackgroundColor({ color: '#4CAF50' });
      
      // 3秒后清除徽章
      setTimeout(() => {
        chrome.action.setBadgeText({ text: '' });
      }, 3000);
    } else {
      // 显示错误徽章
      chrome.action.setBadgeText({ text: '✗' });
      chrome.action.setBadgeBackgroundColor({ color: '#f44336' });
      console.error('同步失败:', result.error);
    }
  }
}

// 启动监控
function startMonitoring() {
  if (isMonitoring) return;

  isMonitoring = true;
  console.log('🚀 开始监控认证 Cookie 变化');

  // 立即检查一次
  monitorToken();

  // 定期检查
  monitorIntervalId = setInterval(() => {
    if (isMonitoring) {
      monitorToken();
    }
  }, CONFIG.checkInterval);
}

// 停止监控
function stopMonitoring() {
  isMonitoring = false;
  if (monitorIntervalId) {
    clearInterval(monitorIntervalId);
    monitorIntervalId = null;
  }
  console.log('⏸️ 停止监控认证 Cookie');
}

// 监听来自 popup 的消息
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'getStatus') {
    getCurrentAuthSnapshot().then((authSnapshot) => {
      sendResponse({
        isMonitoring,
        hasToken: !!authSnapshot?.token,
        hasJsessionid: !!authSnapshot?.jsessionid,
        lastSync: lastSyncAt
      });
    });
    return true; // 保持消息通道开启
  }
  
  if (request.action === 'syncNow') {
    getCurrentAuthSnapshot().then(async (authSnapshot) => {
      if (authSnapshot) {
        const result = await sendTokenToServer(authSnapshot);
        if (result.success) {
          lastCookieSignature = authSnapshot.signature;
          lastSyncAt = new Date().toISOString();
        }
        sendResponse({
          success: result.success,
          error: result.error,
          token: result.success ? authSnapshot.token : null
        });
      } else {
        sendResponse({
          success: false,
          error: '未找到 BigSeller 认证 Cookie。请先访问 https://www.bigseller.pro 并登录'
        });
      }
    });
    return true;
  }
  
  if (request.action === 'toggleMonitoring') {
    if (isMonitoring) {
      stopMonitoring();
    } else {
      startMonitoring();
    }
    sendResponse({ isMonitoring: isMonitoring });
  }
});

// 监听 Cookie 变化（更精确的监控）
chrome.cookies.onChanged.addListener((changeInfo) => {
  if (
    changeInfo.cookie.domain.includes(CONFIG.domain) &&
    CONFIG.monitoredCookieNames.includes(changeInfo.cookie.name)
  ) {
    console.log('🔔 Cookie 变化事件触发');
    monitorToken();
  }
});

// 扩展安装或更新时
chrome.runtime.onInstalled.addListener(() => {
  console.log('📦 BigSeller Token Auto Sync 已安装');
  startMonitoring();
});

// 扩展启动时
chrome.runtime.onStartup.addListener(() => {
  console.log('🔄 BigSeller Token Auto Sync 已启动');
  startMonitoring();
});

// 自动启动监控
startMonitoring();
