# NapCatSharpLib AI 开发指南

## 目录

1. [项目概述](#项目概述)
2. [整体架构](#整体架构)
3. [WebSocket 连接层](#websocket-连接层)
4. [事件系统](#事件系统)
5. [API 调用层](#api-调用层)
6. [CQ 消息构造](#cq-消息构造)
7. [完整使用示例](#完整使用示例)
8. [与 NapCat/go-cqhttp 协议对照](#与-napcatgo-cqhttp-协议对照)
9. [已知问题与注意事项](#已知问题与注意事项)
10. [ruri-bot 中的最佳实践](#ruri-bot-中的最佳实践)

---

## 项目概述

**NapCatSharpLib** 是一个 C# 类库（目标框架 .NET Standard 2.0），用于通过 WebSocket 与 **NapCatQQ**（QQ 机器人框架）进行通信。该库最初为 go-cqhttp 设计，后适配至 NapCat。

核心功能：
- 通过 WebSocket 连接 NapCatQQ 服务端，接收推送事件
- 调用 NapCat API（发送消息、群管理、好友操作等）
- 构造和解析 CQ 码格式的消息

唯一的外部依赖：**Newtonsoft.Json 13.0.1**

项目文件结构：

```
NapCatSharpLib/
├── API/                    # API 调用层
│   ├── NapCatAPI.Base.cs       # API 基类（请求/响应的核心逻辑）
│   ├── NapCatAPI.Common.cs     # 主要 API 类（50+ 方法）
│   ├── NapCatAPI.Advanced.cs   # 高级 API（版本、重启等）
│   ├── NapCatAPI.Request.cs    # 请求模型
│   ├── NapCatAPI.Respond.cs    # 响应模型
│   └── NapCatAPI.Type.cs       # API 相关枚举
├── Data/                   # 数据传输对象（DTO）
│   ├── NapCatData.Base.cs      # 事件数据基类
│   ├── NapCatData.API.cs       # API 返回数据类
│   ├── NapCatData.Message.cs   # 消息事件数据
│   ├── NapCatData.MetaEvent.cs # 元事件数据（生命周期、心跳）
│   ├── NapCatData.Notice.cs    # 通知事件数据（15+ 种通知类型）
│   ├── NapCatData.Request.cs   # 请求事件数据（好友/群请求）
│   ├── NapCatData.Sender.cs    # 发送者信息
│   └── NapCatData.Struct.cs    # 结构体数据
├── Enum/                   # 枚举定义
│   ├── NapCatEnum.Common.cs    # post_type 枚举
│   ├── NapCatEnum.Message.cs   # 消息子类型枚举
│   ├── NapCatEnum.MetaEvent.cs # 元事件类型枚举
│   ├── NapCatEnum.Notice.cs    # 通知类型枚举
│   ├── NapCatEnum.Request.cs   # 请求类型枚举
│   └── NapCatEnum.Sender.cs    # 发送者角色/性别枚举
├── Event/                  # 事件系统
│   ├── NapCatEvent.Message.cs  # 消息事件委托
│   ├── NapCatEvent.MetaEvent.cs# 元事件委托
│   ├── NapCatEvent.Notice.cs   # 通知事件委托
│   ├── NapCatEvent.Request.cs  # 请求事件委托
│   └── Manager/
│       ├── NapCatEventManager.cs   # 事件管理器（持有所有委托）
│       └── NapCatEventAnalyzer.cs  # JSON 解析与事件分发
├── HTTP/                   # HTTP 工具
│   └── NapCatHttpRequest.cs     # 通用 HTTP 请求
├── Message/                # CQ 消息系统
│   ├── CqCode.cs               # CQ 码类型枚举（22 种）
│   ├── CqFaceCode.cs           # QQ 表情 ID 枚举（~200 个）
│   ├── CqMessage.cs            # 单个消息段（type + data 字典）
│   ├── CqMessageBuilder.cs     # 消息构造器（流式 API）
│   ├── CqMessageChain.cs       # 消息链（多个消息段的容器）
│   ├── CqMessageData.cs        # 强类型数据访问器
│   ├── CqMessageLexer.cs       # CQ 字符串解析器
│   └── CqParamsType.cs         # 相关参数枚举
├── WebSocket/
│   └── NapCatWebSocket.cs      # WebSocket 客户端
├── Utility/
│   └── NapCatCharEncoder.cs    # CQ 特殊字符编解码
└── Log/
    └── NapCatLog.cs            # 日志委托
```

---

## 整体架构

### 通信模型

```
┌──────────────────────────────────────────────────────┐
│                    你的应用程序                        │
│                                                      │
│  ┌──────────────┐    ┌───────────────────┐           │
│  │ 订阅事件      │    │ 调用 API 方法      │           │
│  │ (委托 +=)     │    │ (SendGroupMessage  │           │
│  │              │    │  等 async 方法)     │           │
│  └──────┬───────┘    └────────┬──────────┘           │
│         │                     │                      │
│  ┌──────▼─────────────────────▼──────────────────┐   │
│  │              NapCatWebSocket                   │   │
│  │   ┌────────────────┐  ┌──────────────────┐   │   │
│  │   │ EventAnalyzer   │  │ API Request/Resp │   │   │
│  │   │ (JSON→Event)    │  │ (echo 匹配)      │   │   │
│  │   └────────────────┘  └──────────────────┘   │   │
│  │              │                │               │   │
│  └──────────────┼────────────────┼───────────────┘   │
│                 │                │                    │
└─────────────────┼────────────────┼────────────────────┘
                  │                │
          ┌───────▼────────────────▼────────┐
          │         ClientWebSocket          │
          │   ws://{ip}:{port}              │
          └────────────────┬─────────────────┘
                           │
                  ┌────────▼────────┐
                  │   NapCatQQ       │
                  │   (服务端)        │
                  └──────────────────┘
```

### 核心设计模式

**请求-响应关联（echo 机制）：**
1. 每次 API 调用生成随机整数 ID（`GetReqId()`）
2. 该 ID 作为 `echo` 字段写入 JSON 请求
3. 服务端响应也携带相同的 `echo` 字段
4. `NapCatAPIBase.AnalyzeRespond` 将响应存入 `Dictionary<int, NapCatAPIRespondBase>`（key 为 echo）
5. 调用方通过轮询该字典等待响应（15 秒超时）

**观察者模式（事件委托）：**
- `NapCatEventManager` 持有 20+ 个公开的委托字段，每个对应一种事件类型
- `NapCatEventAnalyzer` 解析 WebSocket 推送的 JSON，反序列化为对应数据对象，触发对应委托
- 使用方通过 `+=` 订阅需要的委托

**建造者模式（消息构造）：**
- `CqMessageBuilder` 提供流式 API 构造消息
- 每个 `Add*` 方法创建一个 `CqMessage`（含 `CqCode` 类型和数据字典）
- 所有消息段存储于 `CqMessageChain` 的 `List<CqMessage>` 中

---

## WebSocket 连接层

### NapCatWebSocket

命名空间：`NapCatSharpLib.WebSocket`

这是整个库的入口类，管理与 NapCatQQ 的 WebSocket 连接。

```csharp
// 构造函数
public NapCatWebSocket(string ip, int port, INapCatWSDebugOutput _debugOutput = null)
```

**连接流程：**

```csharp
// 1. 创建实例
var ws = new NapCatWebSocket("127.0.0.1", 3001);

// 2. 启动连接（内部：创建 ClientWebSocket → ConnectAsync → MainLoop 循环接收）
ws.Start();

// 3. 检查连接状态
bool connected = ws.IsConnected;

// 4. 获取事件管理器（订阅事件用）
NapCatEventManager evtMgr = ws.EventManager;
```

**内部工作原理：**

`Start()` 方法调用链：
1. 新建 `ClientWebSocket` 实例
2. 异步连接到 `ws://{ip}:{port}`
3. 进入 `MainLoop()`：循环调用 `ReceiveAsync` 读取消息
4. 每条消息先写入 debug 输出
5. 触发 `OnDataReceive` 事件（供 API 层消费）
6. 传给 `eventAnalyzer.AnalyzeEvent(data)` 解析并分发事件

### INapCatWSDebugOutput 接口

可选的调试输出接口，用于捕获 WebSocket 通信日志：

```csharp
public interface INapCatWSDebugOutput
{
    void Log(string message);
}
```

---

## 事件系统

### 可订阅的事件列表

访问路径：`ws.EventManager.OnEventXxx`

#### 消息事件

| 事件委托 | 数据类型 | 说明 |
|---------|---------|-----|
| `OnEventMessagePrivate` | `NapCatMessagePrivate` | 收到私聊消息 |
| `OnEventMessageGroup` | `NapCatMessageGroup` | 收到群聊消息 |

#### 通知事件

| 事件委托 | 数据类型 | 说明 |
|---------|---------|-----|
| `OnEventNoticeGroupFileUpload` | `NapCatNoticeGroupFileUpload` | 群文件上传 |
| `OnEventNoticeGroupAdminChange` | `NapCatNoticeGroupAdminChange` | 群管理员变更 |
| `OnEventNoticeGroupDecrease` | `NapCatNoticeGroupDecrease` | 群成员减少 |
| `OnEventNoticeGroupIncrease` | `NapCatNoticeGroupIncrease` | 群成员增加 |
| `OnEventNoticeGroupBan` | `NapCatNoticeGroupBan` | 群禁言 |
| `OnEventNoticeAddFriend` | `NapCatNoticeAddFriend` | 好友添加 |
| `OnEventNoticeGroupRecall` | `NapCatNoticeGroupRecall` | 群消息撤回 |
| `OnEventNoticePrivateRecall` | `NapCatNoticePrivateRecall` | 私聊消息撤回 |
| `OnEventNoticePrivatePoke` | `NapCatNoticePrivatePoke` | 私聊戳一戳 |
| `OnEventNoticeGroupPoke` | `NapCatNoticeGroupPoke` | 群聊戳一戳 |
| `OnEventNoticeGroupLuckyKing` | `NapCatNoticeGroupLuckyKing` | 群幸运王 |
| `OnEventNoticeGroupHonor` | `NapCatNoticeGroupHonor` | 群荣誉变更 |
| `OnEventNoticeGroupCardChange` | `NapCatNoticeGroupCardChange` | 群名片变更 |
| `OnEventNoticeOfflineFileReceive` | `NapCatNoticeOfflineFileReceive` | 离线文件接收 |
| `OnEventNoticeClientStatusChange` | `NapCatNoticeClientStatusChange` | 客户端状态变更 |
| `OnEventNoticeGroupEssence` | `NapCatNoticeGroupEssence` | 群精华消息 |

#### 请求事件

| 事件委托 | 数据类型 | 说明 |
|---------|---------|-----|
| `OnEventRequestFriend` | `NapCatRequestFriend` | 好友请求 |
| `OnEventRequestGroup` | `NapCatRequestGroup` | 群请求（加群/邀请） |

#### 元事件

| 事件委托 | 数据类型 | 说明 |
|---------|---------|-----|
| `OnEventMetaLifeCycle` | `NapCatMetaEventLifeCycle` | 生命周期事件（启动/启用/禁用） |
| `OnEventMetaHeartbeat` | `NapCatMetaEventHeartbeat` | 心跳事件 |

**注意：** `NapCatEventAnalyzer` 的 `meta_event` 分支为空实现——生命周期和心跳事件的 JSON 能正确解析但不会被分发。如果需要在 ruri-bot 之外的场景使用元事件，需要修改 `NapCatEventAnalyzer.cs` 第 171 行附近的空分支。

### 通用数据字段

所有事件数据类继承自 `NapCatDataBase`，包含：
- `time` (long) — Unix 时间戳
- `self_id` (long) — 机器人自身的 QQ 号
- `post_type` (NapCatPostType) — 事件大类：`message`、`notice`、`request`、`meta_event`

### 消息事件数据关键字段

`NapCatMessagePrivate`：
- `sub_type` — `friend`、`group`、`group_self`、`other`
- `message_id` (int) — 消息 ID
- `user_id` (long) — 发送者 QQ
- `message` (string) — CQ 码格式的消息文本（含 `[CQ:...]`）
- `raw_message` (string) — 不含 CQ 码的纯文本
- `sender` (NapCatMessageSender) — 发送者信息（nickname、sex、age）
- `temp_source` (NapCatMessageTempSource) — 临时会话来源

`NapCatMessageGroup` 额外包含：
- `group_id` (long) — 群号
- `anonymous` (NapCatAnonymous) — 匿名信息（若为匿名消息）
- `sender.card` — 群名片
- `sender.role` — 群角色（`owner`、`admin`、`member`）

### 基本订阅模式

```csharp
// 直接订阅
ws.EventManager.OnEventMessageGroup += (NapCatMessageGroup msg) =>
{
    Console.WriteLine($"群 {msg.group_id} 收到消息: {msg.raw_message}");
};

// 也可以订阅多个事件
ws.EventManager.OnEventNoticeGroupIncrease += (NapCatNoticeGroupIncrease notice) =>
{
    Console.WriteLine($"群 {notice.group_id} 有新成员加入: {notice.user_id}");
};
```

### 消息字符串的结构

收到的 `message` 字段是 CQ 码字符串，例如：

```
[CQ:at,qq=123456] 你好 [CQ:image,file=abc.jpg]
```

可以用 `CqMessageLexer` 解析为 `CqMessageChain`：

```csharp
var chain = new CqMessageChain(msg.message);
// chain 中包含了三个消息段：at、text、image

foreach (var seg in chain)
{
    if (seg.type == CqCode.text)
    {
        var textData = new CqMessageDataText(seg);
        Console.WriteLine("文本: " + textData.text);
    }
    else if (seg.type == CqCode.at)
    {
        var atData = new CqMessageDataAt(seg);
        Console.WriteLine("At了: " + atData.qq);
    }
}
```

---

## API 调用层

### 创建 API 实例

```csharp
// 必须先创建 WebSocket，再创建 API
var ws = new NapCatWebSocket("127.0.0.1", 3001);
var api = new NapCatAPI(ws);
var apiAdvanced = new NapCatAPIAdvanced(ws);
```

**重要：** `NapCatAPI` 和 `NapCatAPIAdvanced` 的构造函数都需要 `NapCatWebSocket` 实例。它们都继承自 `NapCatAPIBase`，在构造时会订阅 `ws.OnDataReceive` 事件来匹配 API 响应。

两个 API 类**共享同一个 WebSocket 连接**是安全的。

### NapCatAPI 方法列表

#### 用户操作

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `SetQQProfile` | `nickname, company, email, college, personalNote` | `NapCatAPIRespondBase` | 设置个人资料 |
| `GetStrangerInfo` | `userId, noCache` | `NapCatAPIRespondObject<NapCatStrangerInfo>` | 获取陌生人信息 |
| `GetFriendList` | — | `NapCatAPIRespondArray<NapCatFriendInfo>` | 获取好友列表 |
| `GetUniDirectionalFriendList` | — | `NapCatAPIRespondArray<NapCatUniDirectionalFriendInfo>` | 获取单向好友列表 |
| `DeleteFriend` | `userId` | `NapCatAPIRespondBase` | 删除好友 |
| `DeleteUniDirectionalFriend` | `userId` | `NapCatAPIRespondBase` | 删除单向好友 |
| `GetUserStatus` | `userId` | `NapCatAPIRespondObject<NapCatStatus>` | 获取用户在线状态 |
| `UploadPrivateFile` | `userId, filePath, fileName` | `NapCatAPIRespondBase` | 上传私聊文件 |
| `GetOnlineClients` | `noCache` | `NapCatAPIRespondObject<NapCatClients>` | 获取在线客户端列表 |

#### 消息操作（核心）

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `SendGroupMessage` | `groupId, chain` | `NapCatAPIRespondObject<NapCatMessageId>` | **发送群消息** |
| `SendPrivateMessage` | `userId, chain, groupId, autoEscape` | `NapCatAPIRespondObject<NapCatMessageId>` | **发送私聊消息** |
| `RecallMessage` | `messageId` | `NapCatAPIRespondBase` | 撤回消息 |
| `GetMessage` | `messageId` | `NapCatAPIRespondObject<NapCatGetMessage>` | 获取消息详情 |
| `GetForwardMessage` | `messageId` | `NapCatAPIRespondObject<NapCatGetForwardMessage>` | 获取合并转发消息 |
| `SendGroupForwardMessage` | `groupId, messages` | `NapCatAPIRespondObject<NapCatMessageId>` | 发送合并转发消息 |
| `GetImage` | `file` | `NapCatAPIRespondObject<NapCatGetImage>` | 获取图片信息 |
| `GetRecord` | `file, outFormat` | `NapCatAPIRespondObject<NapCatGetRecord>` | 获取语音文件 |
| `MarkMsgAsRead` | `messageId` | `NapCatAPIRespondBase` | 标记消息为已读 |
| `SetMsgEmojiLike` | `messageId, emojiId, set` | `NapCatAPIRespondBase` | 设置消息表情回应 |
| `SendLike` | `userId, times` | `NapCatAPIRespondBase` | 发送点赞 |
| `GetGroupMsgHistory` | `groupId, messageSeq` | `NapCatAPIRespondObject<...>` | 获取群消息历史 |

#### 群操作

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `SetGroupKick` | `groupId, userId, rejectRequest` | `NapCatAPIRespondBase` | 踢出群成员 |
| `SetGroupBan` | `groupId, userId, duration` | `NapCatAPIRespondBase` | 禁言群成员 |
| `SetGroupWholeBan` | `groupId, enable` | `NapCatAPIRespondBase` | 全员禁言 |
| `SetGroupAdmin` | `groupId, userId, enable` | `NapCatAPIRespondBase` | 设置管理员 |
| `SetGroupCard` | `groupId, userId, card` | `NapCatAPIRespondBase` | 设置群名片 |
| `SetGroupName` | `groupId, name` | `NapCatAPIRespondBase` | 设置群名称 |
| `SetGroupLeave` | `groupId, dismiss` | `NapCatAPIRespondBase` | 退出/解散群 |
| `SetGroupSpecialTitle` | `groupId, userId, title, duration` | `NapCatAPIRespondBase` | 设置专属头衔 |
| `SetGroupPortrait` | `groupId, file, cache` | `NapCatAPIRespondBase` | 设置群头像 |
| `GetGroupInfo` | `groupId, noCache` | `NapCatAPIRespondObject<NapCatGroupInfo>` | 获取群信息 |
| `GetGroupMemberInfo` | `groupId, userId, noCache` | `NapCatAPIRespondObject<NapCatGroupMemberInfo>` | 获取群成员信息 |
| `GetGroupMemberList` | `groupId, noCache` | `NapCatAPIRespondArray<NapCatGroupMemberInfo>` | 获取群成员列表 |
| `GetGroupHonorInfo` | `groupId, type` | `NapCatAPIRespondObject<...>` | 获取群荣誉信息 |
| `GetGroupSystemMsg` | `groupId` | `NapCatAPIRespondObject<...>` | 获取群系统消息 |
| `GetGroupEssenceList` | `groupId` | `NapCatAPIRespondArray<...>` | 获取群精华列表 |
| `SetGroupEssence` | `messageId` | `NapCatAPIRespondBase` | 设置精华消息 |
| `DeleteGroupEssence` | `messageId` | `NapCatAPIRespondBase` | 取消精华消息 |
| `GetGroupAtAllRemain` | `groupId` | `NapCatAPIRespondObject<...>` | 获取 @全体成员剩余次数 |
| `GetGroupFileSystemInfo` | `groupId` | `NapCatAPIRespondObject<...>` | 获取群文件系统信息 |
| `GetGroupFileUrl` | `groupId, fileId, busId` | `NapCatAPIRespondObject<...>` | 获取群文件下载链接 |
| `UploadGroupFile` | `groupId, filePath, name, folderId` | `NapCatAPIRespondBase` | 上传群文件 |
| `SetGroupAddRequest` | `flag, type, approve, reason` | `NapCatAPIRespondBase` | 处理加群请求 |
| `GetGroupNotice` | `groupId` | `NapCatAPIRespondArray<...>` | 获取群公告 |
| `SendGroupNotice` | `groupId, title, content` | `NapCatAPIRespondBase` | 发送群公告 |

#### 其他

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `GetLoginInfo` | — | `NapCatAPIRespondObject<NapCatLoginInfo>` | 获取登录信息 |
| `GetCookies` | `domain` | `NapCatAPIRespondObject<NapCatCookies>` | 获取 Cookies |
| `GetCsrfToken` | — | `NapCatAPIRespondObject<NapCatCsrfToken>` | 获取 CSRF Token |
| `GetCredentials` | `domain` | `NapCatAPIRespondObject<NapCatCredentials>` | 获取凭证 |
| `CanSendImage` | — | `NapCatAPIRespondObject<NapCatCanSendImage>` | 检查是否能发送图片 |
| `CanSendRecord` | — | `NapCatAPIRespondObject<NapCatCanSendRecord>` | 检查是否能发送语音 |
| `ImageOCR` | `image` | `NapCatAPIRespondObject<NapCatOcrResult>` | 图片 OCR 识别 |
| `SetFriendAddRequest` | `flag, approve, remark` | `NapCatAPIRespondBase` | 处理好友请求 |
| `GetWordSlices` | `content` | `NapCatAPIRespondObject<...>` | 获取中文分词 |

### NapCatAPIAdvanced 方法

| 方法 | 参数 | 返回 | 说明 |
|------|------|------|------|
| `GetVersionInfo` | — | `NapCatAPIRespondObject<NapCatVersionInfo>` | 获取版本信息 |
| `GetStatus` | — | `NapCatAPIRespondObject<NapCatStatus>` | 获取运行状态 |
| `SetRestart` | `delay` | `NapCatAPIRespondBase` | 重启（延迟毫秒） |
| `CleanCache` | — | `NapCatAPIRespondBase` | 清理缓存 |

### 响应模型

所有 API 方法返回以下三种响应类型之一：

```csharp
// 基类
public class NapCatAPIRespondBase
{
    public NapCatAPIStatus status;  // ok / failed
    public int retcode;             // 0 = 成功
    public string msg;              // 错误信息（如有）
    public string wording;          // 人类可读的错误描述
    public string echo;             // 请求 echo
}

// 泛型版本
public class NapCatAPIRespondObject<T> : NapCatAPIRespondBase
{
    public T data;  // 单对象响应
}

public class NapCatAPIRespondArray<T> : NapCatAPIRespondBase
{
    public List<T> data;  // 数组响应
}
```

**使用方式：**

```csharp
var resp = await api.GetGroupMemberList(123456789);

if (resp.retcode == 0)
{
    foreach (var member in resp.data)
    {
        Console.WriteLine($"{member.nickname} ({member.user_id})");
    }
}
else
{
    Console.WriteLine($"错误: {resp.wording}");
}
```

### API 调用约定

由于 API 调用通过 WebSocket 异步发送/接收，内部使用了轮询等待模式：`SendAPIRequestObjectAsync` 方法发送请求后在 15 秒内轮询 `respMap` 字典，每次轮询间隔 10ms。

所有 API 方法均已标记 `async` 并返回 `Task<T>`。只能在同一 WebSocket 连接建立后调用。

---

## CQ 消息构造

### CqMessageChain（消息链）

消息链是一个**有序的消息段列表**。这是发送消息时使用的标准数据格式。

```csharp
// 创建一个空的消息链
CqMessageChain chain = new CqMessageChain();

// 从 CQ 字符串解析
CqMessageChain chain = new CqMessageChain("[CQ:at,qq=123] 你好");

// 序列化（API 内部调用，通常不需要手动调用）
string cqString = chain.ToCqQuery();  // 转为 CQ 码字符串
string json = chain.ToJson();         // 转为 JSON（用于转发消息）
```

### CqMessageBuilder（建造者）

通过 `chain.Builder` 访问，提供流式 API 构造消息：

```csharp
var chain = new CqMessageChain();
var builder = chain.Builder;

// 文本
builder.AddText("Hello World!");

// QQ 表情（使用 CqFaceCode 枚举，约 200 种表情）
builder.AddFace(CqFaceCode.wei_xiao);
builder.AddFace(CqFaceCode.da_ku);

// @某人（qq=0 或 qq=-1 代表 @全体成员）
builder.AddAt(123456789);
builder.AddAt("all");  // 即 @全体成员

// @某人并显示名称
builder.AddAt(123456789, "用户昵称");

// 回复消息
builder.AddReply(messageId);

// 自定义回复
builder.AddReplyCustom("回复文本", senderQq, unixTime, messageSeq);

// 图片（支持 HTTP URL、本地文件路径、base64）
builder.AddImage("https://example.com/img.jpg");
builder.AddImage("file:///C:/image.png");
builder.AddImage("base64://iVBORw0KGgoAAA...");
builder.AddImage("http://...", CqImageType.flash);  // 闪照

// 语音
builder.AddRecord("file:///audio.amr");
builder.AddRecord("file:///audio.amr", magic: 1, cache: 1, proxy: 1, timeout: 10);

// 视频
builder.AddVideo("file:///video.mp4");
builder.AddVideo("file:///video.mp4", "file:///cover.jpg", 100);  // c=100

// 分享链接
builder.AddShare("https://example.com", "标题", "内容摘要", "https://example.com/img.jpg");

// 音乐分享（平台音乐）
builder.AddMusic(CqMusicType.qq, musicId);
builder.AddMusic(CqMusicType.netease, musicId);

// 自定义音乐
builder.AddMusicCustom("https://music.url", "https://audio.url", "歌曲名", "歌手名", "https://cover.url");

// 合并转发
builder.AddForward(messageId);
builder.AddNode(messageId);  // 转发节点
builder.AddNodeCustom(nickname, qq, chain);  // 自定义转发节点

// XML 消息
builder.AddXml("<?xml ...>", resid: 0);

// JSON 消息（小程序卡片）
builder.AddJson("{\"app\":\"...\"}", resid: 0);

// 卡片图片
builder.AddCardImage(file: "file:///card.jpg", minWidth: 200, minHeight: 200);

// 文字转语音
builder.AddTTS("要朗读的文本");

// 礼物
builder.AddGift(targetQq, giftId);

// 位置
builder.AddLocation(lat: 39.9, lon: 116.4, "位置标题", "位置描述");

// 戳一戳
builder.AddPoke(targetQq);
```

### CqMessage 与 CqMessageData

从接收到的消息中提取结构化信息：

```csharp
CqMessageChain chain = new CqMessageChain(msg.message);

foreach (var seg in chain)
{
    switch (seg.type)
    {
        case CqCode.text:
            var textData = new CqMessageDataText(seg);
            Console.WriteLine("文本: " + textData.text);
            break;

        case CqCode.image:
            var imageData = new CqMessageDataImage(seg);
            Console.WriteLine("图片: " + imageData.url);
            break;

        case CqCode.at:
            var atData = new CqMessageDataAt(seg);
            Console.WriteLine("@了: " + atData.qq);
            break;

        case CqCode.reply:
            var replyData = new CqMessageDataReply(seg);
            Console.WriteLine("回复消息ID: " + replyData.id);
            break;

        case CqCode.face:
            var faceData = new CqMessageDataFace(seg);
            Console.WriteLine("表情ID: " + faceData.id);
            break;
    }
}
```

### CqMessageData 可用类型

在 `NapCatSharpLib\Message\CqMessageData.cs` 中定义，每种对应一个 CQ 码类型：

- `CqMessageDataText` — `text` 字段
- `CqMessageDataFace` — `id`
- `CqMessageDataImage` — `file`、`type`、`url`、`subType`、`fileSize`
- `CqMessageDataRecord` — `file`、`magic`、`url`、`cache`、`proxy`、`timeout`
- `CqMessageDataVideo` — `file`、`url`、`cover`
- `CqMessageDataAt` — `qq`、`name`
- `CqMessageDataShare` — `url`、`title`、`content`、`image`
- `CqMessageDataMusic` — `type`、`id`、`url`、`audio`、`title`、`content`、`image`
- `CqMessageDataReply` — `id`、`text`、`qq`、`time`、`seq`
- `CqMessageDataForward` — `id`
- `CqMessageDataNode` — `id`、`name`、`uin`、`content`、`seq`
- `CqMessageDataXml` — `data`、`resid`
- `CqMessageDataJson` — `data`、`resid`
- `CqMessageDataPoke` — `type`、`id`、`name`
- `CqMessageDataGift` — `qq`、`id`
- `CqMessageDataCardImage` — `file`、`minwidth`、`minheight`、`maxwidth`、`maxheight`、`source`、`icon`
- `CqMessageDataTTS` — `text`
- `CqMessageDataLocation` — `lat`、`lon`、`title`、`content`

### CQ 特殊字符编解码

工具类：`NapCatSharpLib.Utility.NapCatCharEncoder`

```csharp
// 编码（将特殊字符转为 CQ 通配符）
string encoded = NapCatCharEncoder.Encode("含有 &amp; 的文本");

// 解码（将 CQ 通配符还原）
string decoded = NapCatCharEncoder.Decode("含有 &amp;amp; 的文本");

// InCQCode 方法用于判断字符是否在 CQ 码内部
```

---

## 完整使用示例

### 最小可运行示例

```csharp
using NapCatSharpLib.API;
using NapCatSharpLib.Data;
using NapCatSharpLib.Message;
using NapCatSharpLib.WebSocket;

class SimpleBot
{
    static async Task Main()
    {
        // 1. 连接 NapCatQQ
        var ws = new NapCatWebSocket("127.0.0.1", 3001);
        var api = new NapCatAPI(ws);

        // 2. 订阅群消息事件
        ws.EventManager.OnEventMessageGroup += async (msg) =>
        {
            Console.WriteLine($"[群 {msg.group_id}] {msg.sender.nickname}: {msg.raw_message}");

            // 3. 简单回复
            if (msg.raw_message == "ping")
            {
                var reply = new CqMessageChain();
                reply.Builder.AddReply(msg.message_id);
                reply.Builder.AddText("pong!");

                await api.SendGroupMessage(msg.group_id, reply);
            }
        };

        // 4. 订阅私聊消息事件
        ws.EventManager.OnEventMessagePrivate += async (msg) =>
        {
            Console.WriteLine($"[私聊] {msg.sender.nickname}: {msg.raw_message}");

            if (msg.raw_message == "你好")
            {
                var reply = new CqMessageChain();
                reply.Builder.AddFace(CqFaceCode.wei_xiao);
                reply.Builder.AddText("你好呀！");

                await api.SendPrivateMessage(msg.user_id, reply);
            }
        };

        // 5. 启动连接
        ws.Start();

        // 6. 保持运行
        await Task.Delay(-1);
    }
}
```

### 接收消息并解析 CQ 码

```csharp
ws.EventManager.OnEventMessageGroup += async (msg) =>
{
    // 解析消息中的 CQ 码
    var chain = new CqMessageChain(msg.message);

    string textContent = "";
    List<long> atUsers = new List<long>();
    List<string> imageUrls = new List<string>();

    foreach (var seg in chain)
    {
        switch (seg.type)
        {
            case CqCode.text:
                textContent += new CqMessageDataText(seg).text;
                break;
            case CqCode.at:
                atUsers.Add(long.Parse(new CqMessageDataAt(seg).qq));
                break;
            case CqCode.image:
                imageUrls.Add(new CqMessageDataImage(seg).url);
                break;
        }
    }

    Console.WriteLine($"文本: {textContent}");
    Console.WriteLine($"@了 {atUsers.Count} 人");
    Console.WriteLine($"包含 {imageUrls.Count} 张图片");
};
```

### 调用群管理 API

```csharp
// 获取群成员列表
var members = await api.GetGroupMemberList(groupId);
foreach (var m in members.data)
{
    Console.WriteLine($"{m.nickname} ({m.user_id}) - {m.role} - 入群时间: {m.join_time}");
}

// 踢人
await api.SetGroupKick(groupId, targetUserId, rejectRequest: false);

// 禁言 10 分钟
await api.SetGroupBan(groupId, targetUserId, 600);

// 解禁
await api.SetGroupBan(groupId, targetUserId, 0);

// 全员禁言
await api.SetGroupWholeBan(groupId, enable: true);

// 设置群名片
await api.SetGroupCard(groupId, targetUserId, "新群名片");

// 处理加群请求
await api.SetGroupAddRequest(requestFlag, NapCatRequestGroupSubType.add, approve: true, reason: "欢迎");
```

### HTTP 工具

命名空间：`NapCatSharpLib.HTTP`

```csharp
var http = new NapCatHttpRequest();

// GET 请求
(string contentType, string body) = await http.GetAsync("https://api.example.com/data");

// POST JSON
var resp = await http.PostAsync("https://api.example.com/submit", "{\"key\": \"value\"}");

// 下载文件
await http.DownloadAsync("https://example.com/file.zip", "/path/to/save/file.zip");
```

---

## 与 NapCat/go-cqhttp 协议对照

### WebSocket 连接

| 概念 | go-cqhttp | NapCatSharpLib |
|------|-----------|----------------|
| 连接地址 | `ws://host:port` | `new NapCatWebSocket(host, port)` |
| 事件推送 | WebSocket 文本帧 | `Ws.OnDataReceive → EventAnalyzer → EventManager` |
| API 调用 | JSON 帧 + echo | `NapCatAPIBase.SendAPIRequest...Async` |

### CQ 码格式

NapCatSharpLib 使用的 CQ 码格式与 go-cqhttp 标准一致：

```
[类型:CQ码类型,参数1=值1,参数2=值2,...]
```

例如：
- `[CQ:at,qq=123456]` — @某人
- `[CQ:image,file=http://example.com/img.jpg,type=show]` — 图片
- `[CQ:face,id=14]` — QQ 表情

### action 对照（API 方法 → go-cqhttp action）

| NapCatSharpLib 方法 | go-cqhttp action |
|---------------------|------------------|
| `SendGroupMessage` | `send_group_msg` |
| `SendPrivateMessage` | `send_private_msg` |
| `RecallMessage` | `delete_msg` |
| `GetMessage` | `get_msg` |
| `SetGroupKick` | `set_group_kick` |
| `SetGroupBan` | `set_group_ban` |
| `SetGroupWholeBan` | `set_group_whole_ban` |
| `SetGroupAdmin` | `set_group_admin` |
| `SetGroupCard` | `set_group_card` |
| `GetGroupInfo` | `get_group_info` |
| `GetGroupMemberList` | `get_group_member_list` |
| `GetLoginInfo` | `get_login_info` |

---

## 已知问题与注意事项

### 1. 参数拼写错误

`NapCatAPI.Common.cs` 第 881 行 `UploadGroupFile` 方法中：

```csharp
req.AddParam("uesr_id", userId);  // 应为 "user_id"
```

这可能导致群文件上传功能不可用。

### 2. 元事件未被分发

`NapCatEventAnalyzer.cs` 第 171 行的 `meta_event` 分支为空实现。`OnEventMetaLifeCycle` 和 `OnEventMetaHeartbeat` 在代码中存在但永远不会被触发。需要关注元事件时需自行修改。

### 3. API 超时后内存泄漏风险

`NapCatAPIBase.SendAPIRequestObjectAsync` / `SendAPIRequestArrayAsync` 使用 1500 次轮询（间隔 10ms，总计 15 秒超时）。请求超时后，`respMap` 中的条目不会被清理，后续匹配到的响应会不断累积，造成内存泄漏。

**建议：** 当超时发生时主动清理 `respMap` 中的对应条目。

### 4. respMap 非线程安全

`respMap` 是一个普通的 `Dictionary<int, NapCatAPIRespondBase>`，在 WebSocket 接收回调（一个线程）和 API 轮询（另一个线程）之间没有锁保护。高并发场景下可能引发竞态条件。

### 5. 事件委托是字段而非事件

`NapCatEventManager` 中的委托声明为 `public delegate_field` 而非 `public event delegate_field`，这意味着外部代码可以用 `=` 直接替换整个委托链。使用时应始终使用 `+=` 和 `-=` 操作符。

### 6. 构造函数的参数化陷阱

`NapCatAPI` 和 `NapCatAPIAdvanced` 有无参构造函数，但无参版本不会注册 WebSocket 处理器，API 调用永远不会收到响应。**始终使用有参构造函数**：

```csharp
// 正确
var api = new NapCatAPI(ws);

// 错误——不会工作
var api = new NapCatAPI();
```

### 7. 项目文件中的自引用的 HintPath

`.csproj` 中包含一个对自身 DLL 的自引用，编译时应忽略或移除该条目：

```xml
<Reference Include="NapCatSharpLib">
    <HintPath>bin\Debug\netstandard2.0\NapCatSharpLib.dll</HintPath>
</Reference>
```

这不是功能问题，但在某些编译环境下可能导致警告。

### 8. Busy-Wait 轮询模式

API 请求-响应匹配使用 `Task.Run` + 轮询循环，而非 `TaskCompletionSource`。这意味着每个 API 调用都会占用一个线程池线程等待 15 秒的轮询周期。在高并发场景下可能导致线程池饥饿。

---

## ruri-bot 中的最佳实践

ruri-bot 是与 NapCatSharpLib 同作者开发的 QQ 机器人框架，展示了库的实际使用模式。

### 单例模式共享连接

```csharp
// 1. 创建单例 WebSocket
webSocket = new NapCatWebSocket(_ip, _port);

// 2. 创建 API 实例（共享同一连接）
api = new NapCatAPI(webSocket);
apiAdvanced = new NapCatAPIAdvanced(webSocket);

// 3. 将 API 实例传递给所有模块
foreach (var module in modules)
    module.ModuleEntryInit(cmdReg, evtReg, api, io, permission, logger, dataPath);
```

### 事件订阅的分层架构

ruri-bot 使用**两层事件系统**：

```
NapCatWebSocket (原始 JSON)
  → NapCatEventAnalyzer (解析 + 反序列化)
    → NapCatEventManager (原生委托：OnEventMessageGroup 等)
      → ruri-bot EventManager (权限检查 + 二次分发)
        → 模块回调
```

### 消息处理流程

```csharp
// 1. 订阅原始事件
eventManager.Register<NapCatMessageGroup>(permissionDummy, ProcessGroupMessage);

// 2. 解析消息内容
private void ProcessGroupMessage(NapCatMessageGroup msg)
{
    var cmd = commandLexer.MessageLexer(msg.message);  // 解析为命令对象
    if (cmd != null)
        commandManager.ReactPrivate(cmd, msg, out string ret);  // 分发命令
}

// 3. 在模块中构造回复
protected async void OnMyCommand(RRBotCommand command, NapCatMessageGroup source)
{
    CqMessageChain ret = new CqMessageChain();
    ret.Builder.AddReply(source.message_id);
    ret.Builder.AddText("处理结果: ...");
    await api.SendGroupMessage(source.group_id, ret);
}
```

### 命令解析约定

ruri-bot 的命令系统以 `/` 开头，空格分隔参数：

```
/命令 子命令 参数1 参数2 ...
```

非 `/` 开头的消息不会被当作命令处理。

### async void 模式

ruri-bot 中的命令处理器使用 `async void` 签名，这意味着：
- 异常不会被捕获，会导致进程崩溃
- 调用方无法等待命令完成
- 多个命令可能并发执行

对于生产级应用，建议使用 `async Task`，并在事件分发中 `await` 处理器。

### JSON 持久化模式

使用 `IRRBotModuleIO` 接口进行模块数据持久化：

```csharp
// 读取配置
BotCoreDataBase<T> config = io.ReadData<BotCoreDataBase<T>>("config.json");

// 写入配置
io.SaveData(config, "config.json");
```

模块默认配置使用 `Activator.CreateInstance<T>()` 创建，首次启动后通过 JSON 反序列化填充。

---

## 附录：开发环境清单

| 项目 | 要求 |
|------|------|
| 目标框架 | .NET Standard 2.0（兼容 .NET Framework 4.6.1+ 和 .NET Core 2.0+） |
| 语言版本 | C# 7.3 |
| NuGet 依赖 | Newtonsoft.Json 13.0.1 |
| NapCatQQ | 需开启 WebSocket 服务端 |
| 路径引用 | `..\..\NapCatSharpLib\NapCatSharpLib\bin\Release\netstandard2.0\NapCatSharpLib.dll` |
