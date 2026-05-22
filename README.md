# 极致创意 | ComfyUI 图片生成集群管理服务
基于 Flask 开发的 ComfyUI 分布式集群管理服务，支持文生图（Text-to-Image）、图生图（Image-to-Image）任务调度，多 ComfyUI 节点负载均衡，任务状态实时推送（SSE），以及生成图片的管理功能。

## 项目介绍
本服务作为 ComfyUI 的上层管理节点，解决单 ComfyUI 实例算力不足、任务排队效率低的问题：
- 支持多台 ComfyUI 服务器集群管理，自动调度空闲节点处理生成任务
- 提供文生图/图生图 API 接口，完善的参数校验和任务状态管理
- 通过 SSE 实时推送任务完成/失败/取消状态，前端可实时感知
- 内置图片管理功能：生成图片存储、元数据记录、批量删除/下载
- 任务队列机制，支持任务取消、超时处理、历史任务查询

## 技术栈
- 核心语言：Python 3.8+
- Web 框架：Flask
- 并发处理：Python threading、ThreadPoolExecutor
- 通信方式：HTTP/API、SSE（Server-Sent Events）
- 数据存储：本地文件（图片+元数据）、内存缓存（任务状态）
- 依赖：ComfyUI API 对接、base64 图片处理、uuid 任务ID生成

## 快速开始
### 环境要求
- Python 3.8 及以上版本
- 已部署的 ComfyUI 服务（至少1台，需开启 API 访问）
- 依赖安装：
  ```bash
  pip install flask python-dotenv requests
  ```

### 配置说明
1. 项目根目录创建 `config.json`（默认自动生成，也可手动配置）：
   ```json
   {
     "servers": [
       {
         "id": "server_xxxxxx",
         "name": "默认ComfyUI节点",
         "host": "127.0.0.1",
         "port": 8188,
         "enabled": true
       }
     ]
   }
   ```
2. 工作流模板：需在项目目录下放文生图/图生图的 ComfyUI 工作流 JSON 文件（对应 `workflow_templates` 加载逻辑）

### 启动服务
```bash
# 直接启动（默认关闭debug）
python jzcy/app.py

# 开启Debug模式启动
FLASK_DEBUG=1 python jzcy/app.py
```

### 访问服务
- 主页：http://localhost:5050
- <img width="1434" height="655" alt="image" src="https://github.com/user-attachments/assets/df6d2b2c-97a6-4d1e-8e33-b6d929d51e2b" />

## 核心功能
### 1. 服务器集群管理
- 新增/编辑/删除 ComfyUI 服务器节点
- 实时检测节点在线状态、忙碌状态
- 轻量接口快速获取集群概要（在线数/忙碌数/空闲数）

### 2. 图片生成任务
| 任务类型 | 接口地址              | 核心参数                                                     |
| -------- | --------------------- | ------------------------------------------------------------ |
| 文生图   | `/api/generate/text`  | `prompt`（提示词）、`seed`（随机种子）、`width/height`（64-4096）、`steps`（1-100） |
| 图生图   | `/api/generate/image` | `prompt`、`image_data`（base64图片）、`seed`、`steps`（1-100） |

### 3. 任务管理
- 查询所有任务：`/api/tasks`（活跃任务/已完成任务/队列长度）
- 查询单个任务状态：`/api/tasks/<task_id>`
- 取消任务：`/api/tasks/<task_id>`（DELETE 方法）

### 4. 图片管理
- 列出所有生成图片：`/api/images`（含元数据：prompt/生成类型）
- 获取单张图片：`/api/images/<filename>`
- 下载图片：`/api/images/<filename>/download`
- 批量删除图片：`/api/images/batch-delete`（POST 传 `filenames` 列表）

### 5. 实时状态推送
- SSE 事件流接口：`/api/events`
- 支持事件类型：`task_completed`/`task_failed`/`task_cancelled`/`connected`（心跳）

## 关键接口示例
### 文生图请求
```bash
curl -X POST http://localhost:5050/api/generate/text \
-H "Content-Type: application/json" \
-d '{
  "prompt": "a beautiful sunset over the ocean",
  "width": 1024,
  "height": 1024,
  "steps": 8,
  "seed": 123456
}'
```
返回示例：
```json
{
  "success": true,
  "task_id": "xxxx-xxxx-xxxx-xxxx",
  "queue_position": 1
}
```

### 图生图请求（需先上传图片）
```bash
# 1. 上传图片获取base64
curl -X POST http://localhost:5050/api/upload \
-F "file=@/path/to/your/image.png"
# 2. 调用图生图接口
curl -X POST http://localhost:5050/api/generate/image \
-H "Content-Type: application/json" \
-d '{
  "prompt": "add flowers to the image",
  "image_data": "data:image/png;base64,xxxxxx",
  "steps": 4,
  "seed": 789012
}'
```



## 常见问题
1. **启动报错：工作流模板未加载**  
   确认 `workflows` 目录下存在 `text_to_image.json`/`image_to_image.json` 工作流模板文件。
2. **任务提示「等待空闲服务器超时」**  
   检查所有 ComfyUI 节点是否在线（`/api/servers` 接口），或节点是否被其他任务占用。
3. **图生图上传失败**  
   确保上传文件是合法图片格式（PNG/JPG），且 `uploads` 目录有读写权限。
4. **端口5050被占用**  
   修改 `app.py` 最后一行 `app.run` 的 `port` 参数（如 `port=5051`），重启服务即可。

## 核心逻辑说明
1. **任务调度**：`task_worker` 线程循环从队列取任务，自动匹配空闲 ComfyUI 节点，单节点并行处理一个任务。
2. **任务处理**：`process_task` 函数对接 ComfyUI API，提交工作流、等待执行完成、下载生成图片并记录元数据。
3. **状态推送**：通过 SSE 向前端实时推送任务状态，30秒心跳保活连接。
4. **资源清理**：自动清理过期任务，防止内存溢出；删除图片时同步删除元数据文件。
