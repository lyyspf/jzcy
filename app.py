#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ComfyUI Flask Web界面
支持多服务器管理、文生图、图生图、图片管理
"""

import os
import json
import uuid
import time
import threading
import base64
import random
import queue
import requests
import websocket
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import flask
from werkzeug.utils import secure_filename

app = Flask(__name__)

# 配置
CONFIG_FILE = 'config.json'
GENERATED_IMAGES_DIR = 'generated_images'
UPLOAD_FOLDER = 'uploads'

# 确保目录存在
os.makedirs(GENERATED_IMAGES_DIR, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 全局变量
config = {}
server_status = {}  # 服务器状态: {server_id: {'busy': bool, 'last_check': timestamp}}
task_queue = queue.Queue()
active_tasks = {}  # {task_id: task_info}
completed_tasks = {}  # {task_id: task_info}
task_results = {}  # {task_id: result_info}
lock = threading.RLock()
MAX_COMPLETED_TASKS = 500
cancelled_tasks = set()

_servers_summary_cache = {'data': None, 'timestamp': 0}
_SERVERS_SUMMARY_TTL = 8

# SSE 事件推送
sse_clients = []  # 存放所有连接的客户端队列
sse_lock = threading.Lock()

def push_sse_event(event_type, data):
    """向所有连接的SSE客户端推送事件"""
    msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_clients:
            try:
                q.put_nowait(msg)
            except:
                dead.append(q)
        for q in dead:
            sse_clients.remove(q)

# 加载工作流模板
def load_workflow_templates():
    """加载文生图和图生图的工作流模板"""
    templates = {}
    
    # 文生图模板
    text_to_image_path = os.path.join('.uploads', '54accddf-c49e-45ab-8868-f1e872fe0846_z-文生图.json')
    if os.path.exists(text_to_image_path):
        with open(text_to_image_path, 'r', encoding='utf-8') as f:
            templates['text_to_image'] = json.load(f)
    else:
        # 尝试其他路径
        alt_path = '/workspace/.uploads/54accddf-c49e-45ab-8868-f1e872fe0846_z-文生图.json'
        if os.path.exists(alt_path):
            with open(alt_path, 'r', encoding='utf-8') as f:
                templates['text_to_image'] = json.load(f)
    
    # 图生图模板
    image_to_image_path = os.path.join('.uploads', 'd0413a9d-58ce-4ba8-b4f9-d6232e86fe6c_z-图生图.json')
    if os.path.exists(image_to_image_path):
        with open(image_to_image_path, 'r', encoding='utf-8') as f:
            templates['image_to_image'] = json.load(f)
    else:
        # 尝试其他路径
        alt_path = '/workspace/.uploads/d0413a9d-58ce-4ba8-b4f9-d6232e86fe6c_z-图生图.json'
        if os.path.exists(alt_path):
            with open(alt_path, 'r', encoding='utf-8') as f:
                templates['image_to_image'] = json.load(f)
    
    return templates

workflow_templates = {}

def load_config():
    """加载配置文件"""
    global config
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        config = {
            "servers": [],
            "generated_images_dir": GENERATED_IMAGES_DIR,
            "max_queue_size": 100,
            "server_check_interval": 5
        }
        save_config()
    return config

def save_config():
    """保存配置文件"""
    with lock:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)

def get_server_url(server):
    """获取服务器URL"""
    return f"http://{server['host']}:{server['port']}"

def get_ws_url(server):
    """获取WebSocket URL"""
    return f"ws://{server['host']}:{server['port']}/ws"

def check_server_status(server):
    """检查服务器是否在线"""
    url = get_server_url(server)
    try:
        response = requests.get(f"{url}/system_stats", timeout=2)
        return response.status_code == 200
    except:
        try:
            response = requests.get(f"{url}/queue", timeout=2)
            return response.status_code in (200, 404)
        except:
            return False

def check_server_queue(server):
    """从ComfyUI获取真实队列状态，返回 (在线, 忙碌)"""
    url = get_server_url(server)
    try:
        response = requests.get(f"{url}/queue", timeout=3)
        if response.status_code == 200:
            data = response.json()
            running = len(data.get('queue_running') or [])
            pending = len(data.get('queue_pending') or [])
            return True, (running > 0 or pending > 0)
        return True, False
    except Exception:
        return False, False

def get_idle_server():
    """获取空闲的服务器（先在锁外检测，再在锁内分配并标记busy）"""
    # 先在锁外收集服务器列表副本
    servers_snapshot = []
    with lock:
        servers_snapshot = [s.copy() for s in config.get('servers', []) if s.get('enabled', False)]

    # 锁外逐个检测状态（避免网络请求阻塞锁）
    for server in servers_snapshot:
        online, busy = check_server_queue(server)
        if not online or busy:
            continue
        # 找到空闲服务器，加锁分配并立即标记busy
        with lock:
            server_id = server['id']
            if server_id not in server_status:
                server_status[server_id] = {'busy': False, 'last_check': 0}
            # 双重检查：再次确认没有被其他线程分配
            if server_status[server_id]['busy']:
                continue
            server_status[server_id]['busy'] = True
            server_status[server_id]['last_check'] = time.time()
            return server
    return None

def set_server_busy(server_id, busy):
    """设置服务器忙碌状态"""
    with lock:
        if server_id not in server_status:
            server_status[server_id] = {'busy': False, 'last_check': 0}
        server_status[server_id]['busy'] = busy
        server_status[server_id]['last_check'] = time.time()

def cleanup_old_tasks():
    """清理过期的已完成任务，防止内存泄漏
    
    注意：此函数假设调用者已持有 lock，内部不再加锁
    """
    while len(completed_tasks) > MAX_COMPLETED_TASKS:
        oldest_key = next(iter(completed_tasks))
        task_results.pop(oldest_key, None)
        del completed_tasks[oldest_key]

def update_text_to_image_workflow(workflow, prompt, seed=None, width=1024, height=1024, steps=8):
    """更新文生图工作流参数"""
    workflow = json.loads(json.dumps(workflow))  # 深拷贝
    
    # 更新prompt
    for node_id, node in workflow.items():
        if node.get('class_type') == 'CLIPTextEncode':
            node['inputs']['text'] = prompt
        elif node.get('class_type') == 'EmptySD3LatentImage':
            node['inputs']['width'] = width
            node['inputs']['height'] = height
        elif node.get('class_type') == 'KSampler':
            if seed is not None:
                node['inputs']['seed'] = seed
            else:
                node['inputs']['seed'] = random.randint(0, 2**32 - 1)
            node['inputs']['steps'] = steps
    
    return workflow

def update_image_to_image_workflow(workflow, prompt, image_data, seed=None, steps=4):
    """更新图生图工作流参数"""
    workflow = json.loads(json.dumps(workflow))  # 深拷贝
    
    # 更新prompt和图像
    for node_id, node in workflow.items():
        if node.get('class_type') == 'PrimitiveStringMultiline':
            node['inputs']['value'] = prompt
        elif node.get('class_type') == 'KSampler':
            if seed is not None:
                node['inputs']['seed'] = seed
            else:
                node['inputs']['seed'] = random.randint(0, 2**32 - 1)
            node['inputs']['steps'] = steps
        elif node.get('class_type') == 'LoadImage':
            # 图像会在上传后设置
            pass
    
    return workflow

def upload_image_to_comfyui(server, image_data, filename):
    """上传图片到ComfyUI服务器"""
    url = get_server_url(server)
    
    # 如果是base64编码的图片
    if image_data.startswith('data:image'):
        image_data = image_data.split(',')[1]
    
    image_bytes = base64.b64decode(image_data)
    
    files = {
        'image': (filename, image_bytes, 'image/png'),
        'overwrite': 'true'
    }
    
    response = requests.post(f"{url}/upload/image", files=files, timeout=30)
    if response.status_code == 200:
        return response.json().get('name', filename)
    return None

def queue_prompt(server, workflow, client_id):
    """提交工作流到ComfyUI"""
    url = get_server_url(server)
    payload = {
        "prompt": workflow,
        "client_id": client_id
    }
    try:
        response = requests.post(f"{url}/prompt", json=payload, timeout=30)
        if response.status_code != 200:
            return {'error': f'HTTP {response.status_code}', 'node_errors': {}}
        return response.json()
    except Exception as e:
        return {'error': str(e)}

def wait_for_completion(server, prompt_id, client_id, timeout=300):
    """等待任务完成"""
    ws_url = get_ws_url(server)
    ws = websocket.create_connection(f"{ws_url}?clientId={client_id}", timeout=5)
    
    start_time = time.time()
    try:
        while time.time() - start_time < timeout:
            try:
                message = ws.recv()
                if message:
                    data = json.loads(message)
                    if data.get('type') == 'executing':
                        exec_data = data.get('data', {})
                        if exec_data.get('node') is None and exec_data.get('prompt_id') == prompt_id:
                            ws.close()
                            return True
            except websocket.WebSocketTimeoutException:
                pass
            except Exception as e:
                break
    finally:
        try:
            ws.close()
        except:
            pass
    return False

def get_history(server, prompt_id):
    """获取任务历史"""
    url = get_server_url(server)
    try:
        response = requests.get(f"{url}/history/{prompt_id}", timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None

def download_and_save_image(server, filename, subfolder="", output_path=None):
    """从ComfyUI下载并保存图片"""
    url = get_server_url(server)
    params = {"filename": filename, "type": "output"}
    if subfolder:
        params["subfolder"] = subfolder
    
    try:
        response = requests.get(f"{url}/view", params=params, timeout=30)
        if response.status_code == 200:
            if output_path:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                return output_path
            return response.content
    except Exception as e:
        print(f"下载图片失败: {e}")
    return None

def process_task(task):
    """处理单个任务"""
    task_id = task['task_id']
    task_type = task['type']
    server = task['server']
    client_id = str(uuid.uuid4())

    with lock:
        if task_id in cancelled_tasks:
            cancelled_tasks.discard(task_id)
            set_server_busy(server['id'], False)
            return

    try:
        set_server_busy(server['id'], True)
        
        with lock:
            if task_id in active_tasks:
                active_tasks[task_id]['status'] = 'processing'
                active_tasks[task_id]['started_at'] = datetime.now().isoformat()
                active_tasks[task_id]['server_name'] = server.get('name', server.get('host', '未知'))
        
        if task_type == 'text_to_image':
            workflow = update_text_to_image_workflow(
                workflow_templates['text_to_image'],
                task['prompt'],
                task.get('seed'),
                task.get('width', 1024),
                task.get('height', 1024),
                task.get('steps', 8)
            )
        elif task_type == 'image_to_image':
            # 先上传图片
            image_filename = f"input_{task_id}.png"
            uploaded_name = upload_image_to_comfyui(server, task['image_data'], image_filename)
            
            if not uploaded_name:
                raise Exception("图片上传失败")
            
            workflow = update_image_to_image_workflow(
                workflow_templates['image_to_image'],
                task['prompt'],
                task['image_data'],
                task.get('seed'),
                task.get('steps', 4)
            )
            
            # 更新LoadImage节点
            for node_id, node in workflow.items():
                if node.get('class_type') == 'LoadImage':
                    node['inputs']['image'] = f"{uploaded_name} [input]"
        
        # 提交工作流
        result = queue_prompt(server, workflow, client_id)
        prompt_id = result.get('prompt_id')
        
        if not prompt_id:
            raise Exception(f"提交工作流失败: {result}")
        
        # 等待完成
        completed = wait_for_completion(server, prompt_id, client_id)
        if not completed:
            # WebSocket 等待失败，通过 history 二次确认任务是否实际完成
            history = get_history(server, prompt_id)
            if not (history and prompt_id in history):
                raise Exception("任务超时")

        # 获取输出
        history = get_history(server, prompt_id)
        images = []

        if history and prompt_id in history:
            outputs = history[prompt_id].get('outputs', {})
            for node_id, output in outputs.items():
                if 'images' in output:
                    for img_info in output['images']:
                        filename = img_info['filename']
                        subfolder = img_info.get('subfolder', '')

                        # 下载并保存图片
                        local_filename = f"{task_id}_{filename}"
                        local_path = os.path.join(GENERATED_IMAGES_DIR, local_filename)

                        downloaded = download_and_save_image(server, filename, subfolder, local_path)
                        if downloaded:
                            images.append({
                                'filename': local_filename,
                                'original_filename': filename,
                                'path': local_path
                            })
                            # 保存图片元数据（prompt等）
                            meta_path = os.path.join(GENERATED_IMAGES_DIR, local_filename + '.meta.json')
                            meta = {'prompt': task.get('prompt', ''), 'type': task.get('type', '')}
                            with open(meta_path, 'w', encoding='utf-8') as mf:
                                json.dump(meta, mf, ensure_ascii=False)

        # 保存结果
        with lock:
            task_results[task_id] = {
                'status': 'completed',
                'images': images,
                'prompt_id': prompt_id,
                'completed_at': datetime.now().isoformat()
            }
            if task_id in active_tasks:
                active_tasks[task_id]['status'] = 'completed'
                completed_tasks[task_id] = active_tasks.pop(task_id)
            cleanup_old_tasks()

        push_sse_event('task_completed', {'task_id': task_id, 'images': len(images)})

    except Exception as e:
        with lock:
            if task_id in active_tasks:
                active_tasks[task_id]['status'] = 'failed'
                active_tasks[task_id]['error'] = str(e)
                completed_tasks[task_id] = active_tasks.pop(task_id)
            task_results[task_id] = {
                'status': 'failed',
                'error': str(e)
            }

        push_sse_event('task_failed', {'task_id': task_id, 'error': str(e)})
    finally:
        set_server_busy(server['id'], False)

def task_worker():
    """任务处理工作线程（并行：每个空闲节点同时处理一个任务）"""
    while True:
        try:
            task = task_queue.get(timeout=1)
            if task is None:
                continue
            
            server = None
            wait_count = 0
            while server is None:
                server = get_idle_server()
                if server is None:
                    time.sleep(2)
                    wait_count += 1
                    if wait_count > 150:
                        with lock:
                            if task['task_id'] in active_tasks:
                                active_tasks[task['task_id']]['status'] = 'failed'
                                active_tasks[task['task_id']]['error'] = '等待空闲服务器超时'
                                completed_tasks[task['task_id']] = active_tasks.pop(task['task_id'])
                                cleanup_old_tasks()
                        push_sse_event('task_failed', {'task_id': task['task_id'], 'error': '等待空闲服务器超时'})
                        server = None
                        break
            
            if server is None:
                continue
            
            task['server'] = server
            threading.Thread(target=process_task, args=(task,), daemon=True).start()
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"任务处理错误: {e}")

# 启动工作线程
worker_thread = threading.Thread(target=task_worker, daemon=True)
worker_thread.start()

# ==================== 路由 ====================

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/servers', methods=['GET'])
def get_servers():
    """获取服务器列表（并行检测在线状态，busy用内部标记）"""
    with lock:
        servers_config = list(config.get('servers', []))
    
    def check_one(server):
        online = check_server_status(server)
        return server['id'], online
    
    online_map = {}
    with ThreadPoolExecutor(max_workers=len(servers_config) or 1) as executor:
        futures = {executor.submit(check_one, s): s for s in servers_config}
        for future in as_completed(futures, timeout=6):
            try:
                sid, online = future.result()
                online_map[sid] = online
            except:
                pass
    
    servers = []
    for server in servers_config:
        server_info = server.copy()
        server_info['online'] = online_map.get(server['id'], False)
        with lock:
            status = server_status.get(server['id'], {'busy': False})
            server_info['busy'] = status['busy']
        servers.append(server_info)
    return jsonify({'servers': servers})

@app.route('/api/cluster/status', methods=['GET'])
def get_servers_summary():
    """轻量接口：返回服务器概要状态，用于顶栏快速刷新（带缓存）"""
    now = time.time()
    if _servers_summary_cache['data'] and now - _servers_summary_cache['timestamp'] < _SERVERS_SUMMARY_TTL:
        return jsonify(_servers_summary_cache['data'])

    with lock:
        servers_config = list(config.get('servers', []))
    enabled_servers = [s for s in servers_config if s.get('enabled', False)]

    if not enabled_servers:
        result = {'online': 0, 'busy': 0, 'idle': 0, 'total': 0}
        _servers_summary_cache['data'] = result
        _servers_summary_cache['timestamp'] = now
        return jsonify(result)

    def check_one(server):
        online, busy = check_server_queue(server)
        return server['id'], online, busy

    check_map = {}
    with ThreadPoolExecutor(max_workers=len(enabled_servers)) as executor:
        futures = {executor.submit(check_one, s): s for s in enabled_servers}
        for future in as_completed(futures, timeout=6):
            try:
                sid, online, busy = future.result()
                check_map[sid] = (online, busy)
            except:
                pass

    online_count = 0
    busy_count = 0
    idle_count = 0
    with lock:
        for sid, (online, busy) in check_map.items():
            if online:
                online_count += 1
                status = server_status.get(sid, {'busy': False})
                if status['busy'] or busy:
                    busy_count += 1
                else:
                    idle_count += 1

    result = {
        'online': online_count,
        'busy': busy_count,
        'idle': idle_count,
        'total': len(enabled_servers)
    }
    _servers_summary_cache['data'] = result
    _servers_summary_cache['timestamp'] = now
    return jsonify(result)

@app.route('/api/servers', methods=['POST'])
def add_server():
    """添加服务器"""
    data = request.json
    server = {
        'id': f"server_{uuid.uuid4().hex[:8]}",
        'name': data.get('name', '新服务器'),
        'host': data.get('host', '127.0.0.1'),
        'port': data.get('port', 8188),
        'enabled': True
    }
    with lock:
        config.setdefault('servers', []).append(server)
        save_config()
    return jsonify({'success': True, 'server': server})

@app.route('/api/servers/<server_id>', methods=['PUT'])
def update_server(server_id):
    """更新服务器配置"""
    data = request.json
    allowed_fields = {'name', 'host', 'port', 'enabled'}
    filtered_data = {k: v for k, v in data.items() if k in allowed_fields}
    with lock:
        for i, server in enumerate(config.get('servers', [])):
            if server['id'] == server_id:
                config['servers'][i].update(filtered_data)
                save_config()
                return jsonify({'success': True})
    return jsonify({'success': False, 'error': '服务器不存在'}), 404

@app.route('/api/servers/<server_id>', methods=['DELETE'])
def delete_server(server_id):
    """删除服务器"""
    with lock:
        config['servers'] = [s for s in config.get('servers', []) if s['id'] != server_id]
        save_config()
    return jsonify({'success': True})

@app.route('/api/generate/text', methods=['POST'])
def generate_text_to_image():
    """文生图"""
    if 'text_to_image' not in workflow_templates:
        return jsonify({'success': False, 'error': '文生图工作流模板未加载'}), 400
    
    data = request.json
    task_id = str(uuid.uuid4())

    # 参数校验
    width = data.get('width', 1024)
    height = data.get('height', 1024)
    steps = data.get('steps', 8)
    if not (64 <= width <= 4096) or not (64 <= height <= 4096):
        return jsonify({'success': False, 'error': '图片尺寸须在 64-4096 之间'}), 400
    if not (1 <= steps <= 100):
        return jsonify({'success': False, 'error': '采样步数须在 1-100 之间'}), 400

    task = {
        'task_id': task_id,
        'type': 'text_to_image',
        'prompt': data.get('prompt', ''),
        'seed': data.get('seed'),
        'width': width,
        'height': height,
        'steps': steps,
        'created_at': datetime.now().isoformat()
    }
    
    with lock:
        active_tasks[task_id] = {
            'task_id': task_id,
            'type': 'text_to_image',
            'status': 'queued',
            'prompt': task['prompt'],
            'created_at': task['created_at']
        }
    
    task_queue.put(task)
    
    return jsonify({
        'success': True,
        'task_id': task_id,
        'queue_position': task_queue.qsize()
    })

@app.route('/api/generate/image', methods=['POST'])
def generate_image_to_image():
    """图生图"""
    if 'image_to_image' not in workflow_templates:
        return jsonify({'success': False, 'error': '图生图工作流模板未加载'}), 400
    
    data = request.json
    task_id = str(uuid.uuid4())

    steps = data.get('steps', 4)
    if not (1 <= steps <= 100):
        return jsonify({'success': False, 'error': '采样步数须在 1-100 之间'}), 400

    task = {
        'task_id': task_id,
        'type': 'image_to_image',
        'prompt': data.get('prompt', ''),
        'image_data': data.get('image_data'),
        'seed': data.get('seed'),
        'steps': steps,
        'created_at': datetime.now().isoformat()
    }
    
    with lock:
        active_tasks[task_id] = {
            'task_id': task_id,
            'type': 'image_to_image',
            'status': 'queued',
            'prompt': task['prompt'],
            'created_at': task['created_at']
        }
    
    task_queue.put(task)
    
    return jsonify({
        'success': True,
        'task_id': task_id,
        'queue_position': task_queue.qsize()
    })

@app.route('/api/events')
def sse_stream():
    """SSE 事件流：实时推送任务状态变化"""
    def generate():
        q = queue.Queue(maxsize=50)
        with sse_lock:
            sse_clients.append(q)
        try:
            # 发送初始连接确认
            yield "event: connected\ndata: {\"status\":\"ok\"}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"  # 30秒心跳
        except GeneratorExit:
            pass
        finally:
            with sse_lock:
                if q in sse_clients:
                    sse_clients.remove(q)

    return app.response_class(
        flask.stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )

@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    """获取任务列表"""
    with lock:
        return jsonify({
            'active': list(active_tasks.values()),
            'completed': list(completed_tasks.values()),
            'queue_size': task_queue.qsize()
        })

@app.route('/api/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """获取任务状态"""
    with lock:
        if task_id in active_tasks:
            task = active_tasks[task_id].copy()
            task['results'] = task_results.get(task_id)
            return jsonify(task)
        elif task_id in completed_tasks:
            task = completed_tasks[task_id].copy()
            task['results'] = task_results.get(task_id)
            return jsonify(task)
    return jsonify({'error': '任务不存在'}), 404

@app.route('/api/tasks/<task_id>', methods=['DELETE'])
def cancel_task(task_id):
    """取消任务"""
    with lock:
        cancelled_tasks.add(task_id)
        if task_id in active_tasks:
            del active_tasks[task_id]
    push_sse_event('task_cancelled', {'task_id': task_id})
    return jsonify({'success': True})

@app.route('/api/images/count', methods=['GET'])
def image_count():
    """轻量接口：只返回图片数量，用于前端快速检测是否有新图片"""
    try:
        count = sum(1 for f in os.listdir(GENERATED_IMAGES_DIR)
                    if not f.endswith('.meta.json') and os.path.isfile(os.path.join(GENERATED_IMAGES_DIR, f)))
    except Exception:
        count = 0
    return jsonify({'count': count})

@app.route('/api/images', methods=['GET'])
def list_images():
    """列出所有生成的图片"""
    images = []
    for filename in os.listdir(GENERATED_IMAGES_DIR):
        if filename.endswith('.meta.json'):
            continue
        filepath = os.path.join(GENERATED_IMAGES_DIR, filename)
        if os.path.isfile(filepath):
            stat = os.stat(filepath)
            img_info = {
                'filename': filename,
                'size': stat.st_size,
                'created_at': datetime.fromtimestamp(stat.st_ctime).isoformat()
            }
            # 读取元数据
            meta_path = filepath + '.meta.json'
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, 'r', encoding='utf-8') as mf:
                        meta = json.load(mf)
                        img_info['prompt'] = meta.get('prompt', '')
                        img_info['type'] = meta.get('type', '')
                except Exception:
                    pass
            images.append(img_info)
    images.sort(key=lambda x: x['created_at'], reverse=True)
    return jsonify({'images': images})

@app.route('/api/images/<filename>', methods=['GET'])
def get_image(filename):
    """获取图片"""
    return send_from_directory(GENERATED_IMAGES_DIR, filename)

@app.route('/api/images/<filename>', methods=['DELETE'])
def delete_image(filename):
    """删除图片"""
    filepath = os.path.join(GENERATED_IMAGES_DIR, filename)
    # 防止路径遍历
    if not os.path.abspath(filepath).startswith(os.path.abspath(GENERATED_IMAGES_DIR)):
        return jsonify({'success': False, 'error': '非法文件名'}), 400
    if os.path.exists(filepath):
        os.remove(filepath)
        # 同时删除元数据文件
        meta_path = filepath + '.meta.json'
        if os.path.exists(meta_path):
            os.remove(meta_path)
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': '图片不存在'}), 404

@app.route('/api/images/<filename>/download', methods=['GET'])
def download_image(filename):
    """下载图片"""
    filepath = os.path.join(GENERATED_IMAGES_DIR, filename)
    if not os.path.abspath(filepath).startswith(os.path.abspath(GENERATED_IMAGES_DIR)):
        return jsonify({'success': False, 'error': '非法文件名'}), 400
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True, download_name=filename)
    return jsonify({'success': False, 'error': '图片不存在'}), 404

@app.route('/api/images/batch-delete', methods=['POST'])
def batch_delete_images():
    """批量删除图片"""
    data = request.json
    filenames = data.get('filenames', [])
    deleted = []
    failed = []
    safe_dir = os.path.abspath(GENERATED_IMAGES_DIR)

    for filename in filenames:
        filepath = os.path.join(GENERATED_IMAGES_DIR, filename)
        if not os.path.abspath(filepath).startswith(safe_dir):
            failed.append(filename)
            continue
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                meta_path = filepath + '.meta.json'
                if os.path.exists(meta_path):
                    os.remove(meta_path)
                deleted.append(filename)
            except Exception:
                failed.append(filename)
        else:
            failed.append(filename)

    return jsonify({'deleted': deleted, 'failed': failed})

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """上传文件（用于图生图）"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': '没有文件'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': '没有选择文件'}), 400
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, f"{uuid.uuid4().hex[:8]}_{filename}")
    file.save(filepath)
    
    # 转换为base64
    with open(filepath, 'rb') as f:
        image_data = base64.b64encode(f.read()).decode()
    
    return jsonify({
        'success': True,
        'filepath': filepath,
        'image_data': f"data:image/png;base64,{image_data}"
    })

if __name__ == '__main__':
    # 加载配置
    load_config()
    
    # 加载工作流模板
    workflow_templates = load_workflow_templates()
    print(f"已加载工作流模板: {list(workflow_templates.keys())}")
    
    # 创建模板目录
    os.makedirs('templates', exist_ok=True)
    
    app.run(host='0.0.0.0', port=5050, debug=os.environ.get('FLASK_DEBUG', '0') == '1', threaded=True)
