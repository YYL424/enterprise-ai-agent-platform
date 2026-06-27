# 1. AOP(面向切面编程)
aspect.py 是整个安全防护模块的核心枢纽，它采用了 AOP（面向切面编程）的设计理念。它的主要职责是将“安全审计”与“业务逻辑”完全解耦，使得底层工具在执行时无需关心安全校验的具体细节。

aspect.py 本身不实现任何具体的底层防御算法（例如它不直接操作 Redis 数据库或计算 MD5 指纹），而是作为一个纯粹的调度代理层。这种依赖注入（通过参数传入 rate_limiter 和 path_analyzer）的设计保证了模块极高的可扩展性。若后续需要增加新的安全规则（如数据脱敏、越权访问拦截），只需在阶段一中补充新的策略实例即可，无需修改现有的业务代码或切面主体逻辑。
## 1. 统一的中断信号：SecurityInterceptionException
```python
class SecurityInterceptionException(Exception):
    """当触发任何安全护栏（如熔断、死循环、越权拦截）时抛出。
    成员 A 的状态机会专门 catch 这个异常来挂起任务。"""
    pass
```
这个自定义异常是安全层专属的阻断信号。当任何下游的安全护栏（如频率超限、死循环等）被触发时，都会抛出这个异常。该设计的直接目的是让外层的 Agent 状态机专门捕获它，以便明确区分“安全拦截”与“普通业务代码报错”，从而决定是挂起任务还是执行特定的降级策略。
## 2. 全链路安全切面：audit_tool_call 装饰器
这是该模块的核心高阶函数，负责在工具调用的前后强制植入调度与检查逻辑。
```python
def audit_tool_call(rate_limiter=None, path_analyzer=None):
    """
    企业级全链路 AOP 安全审计切面 (高阶装饰器)
    
    :param rate_limiter: 注入的滑动窗口限流器实例 (如 window.py 中的类)
    :param path_analyzer: 注入的有向拓扑死循环检测器实例 (如 path_hash.py 中的类)
    """
    def decorator(func):
        # 【架构强约束】：@traceable 必须在最外层包裹
        # 这样才能在 LangSmith 上精准录制到被安全策略强杀的 Error 红色链路
        @traceable(name=f"Tool_Execution_{func.__name__}")
        @wraps(func)
        def wrapper(*args, **kwargs):
            tool_name = func.__name__
            
            # 【租户隔离】：企业级生产环境必须有 session_id，不能搞全局大锅饭
            # 假设上层调用工具时，会在 kwargs 里传入 session_id，如果没传则给个兜底
            session_id = kwargs.get("session_id", "anonymous_session")
            ......
            return result
        return wrapper
    return decorator
```
- **强约束的链路追踪 (LangSmith Integration)：**
系统在架构层面强制要求 @traceable 装饰器必须包裹在自定义装饰器的最外层。这种约束是为了确保当安全策略（如熔断器）强行中断代码执行时，异常事件能够被 LangSmith 完整捕获，在监控面板上精准录制为被阻断的红色链路。

- **租户物理隔离 (Tenant Isolation)：**
在拦截到请求时，装饰器会主动从 kwargs 中提取 session_id，如果未提供则回退使用 anonymous_session。这是企业级多租户架构的基础，确保所有的频控限流和死循环检测都严格限制在单个会话维度，避免全局数据污染导致的误拦截。
## 3. 三段式执行生命周期
被该装饰器包裹的工具，在每次被调用时都会严格经历以下三个执行阶段：
- **阶段一：前置安全审查 (Pre-execution Audit)**
```python
            logging.info(f"[AOP 前置审计] 拦截到工具调用请求 | 目标: {tool_name} | 会话: {session_id}")
            t0 = time.perf_counter()

            # ==========================================
            # 1. 前置安全审查 (Pre-execution Audit)
            # ==========================================
            # 指挥调度：让专业的模块干专业的事
            if rate_limiter:
                # 如果超出阈值，rate_limiter 内部会抛出 SecurityInterceptionException
                rate_limiter.check_and_record(session_id, tool_name)

            if path_analyzer:
                # 如果判定为隐蔽交替死循环，内部同样抛出异常强行斩断
                path_analyzer.check_and_record(session_id, tool_name)
```
在实际的业务代码运行前，切面会依次调用注入的 rate_limiter（滑动窗口限流器）和 path_analyzer（路径指纹检测器）。这是典型的指挥调度模式，让专业的模块执行专业的校验。如果任何一个检测器判定当前调用存在风险，其内部会直接抛出 SecurityInterceptionException，强行斩断调用链，后续代码即刻终止。

- **阶段二：物理隔离放行 (Execution)**
```python
            # 2. 物理隔离放行 (Execution)
            # ==========================================
            try:
                # 如果前面的安检全部通过，才真正执行业务代码
                result = func(*args, **kwargs)
            except Exception as e:
                # 区分【安全拦截】与【业务本身报错】
                logging.error(f"[业务运行异常] 工具 {tool_name} 内部执行崩溃: {str(e)}")
                raise e
```
只有当所有的前置安检均通过时，系统才会真正执行 func(*args, kwargs)。在此处，代码使用 try-except 捕获了常规的 Exception，并在日志中明确标记为 [业务运行异常]，以防止开发人员在排查问题时将其与前置的安全拦截混淆。

- **阶段三：后置审计 (Post-execution Audit)**
```python
            # 3. 后置审计 (Post-execution Audit)
            # ==========================================
            latency_ms = (time.perf_counter() - t0) * 1000
            logging.info(f"[AOP 后置审计] 工具 {tool_name} 执行完毕 | 耗时: {latency_ms:.2f}ms")
```
如果工具正常执行完毕未发生崩溃，装饰器会通过高精度时间戳计算工具的实际执行耗时（毫秒级），并输出后置审计通过的日志，完成单次调用的闭环。
---
# 2. 针对分布式高并发场景的物理层算法实现
window.py 和 path_hash.py 绝不是空洞的接口或伪代码，它们是非常具体、且针对分布式高并发场景深度优化过的物理层算法实现。

这两个文件分别落地了两种经典的后端治理算法，以下是它们具体的实现逻辑拆解：
## 1. window.py：基于 Redis ZSET 的分布式滑动窗口算法
它并没有采用最简单的“固定窗口计数器”（Fixed Window Counter，即简单的 INCR 命令），因为固定窗口存在著名的“临界点突刺”漏洞。这段代码实现的是更高级的滑动时间窗口（Sliding Window）。

- **数据结构依赖：** 深度依赖 Redis 的有序集合（Sorted Set / ZSET）。
```python
    """
    企业级基于 Redis Zset 的滑动时间窗口限流器
    """
    def __init__(self, redis_client: Redis, window_seconds: int = 180, max_calls: int = 5):
        """
        初始化安检仪
        :param redis_client: 注入的 Redis 客户端连接 (强制要求连到 DB 2)
        :param window_seconds: 滑动窗口的时间跨度（秒），默认 3 分钟
        :param max_calls: 窗口期内允许的最大连续调用次数，默认 5 次
        """
        self.redis_client = redis_client
        self.window_seconds = window_seconds
        self.max_calls = max_calls
     def check_and_record(self, session_id: str, tool_name: str) -> bool:
        """
        执行核心安检逻辑：清理过期数据 -> 打卡 -> 统计 -> 拦截决策
        如果触发熔断，直接抛出异常斩断控制流。
        """
        # 1. 构建租户隔离的 Redis Key
        # 格式例如: rate_limit:user_001:compile_code
        redis_key = f"rate_limit:{session_id}:{tool_name}"
        
        # 2. 获取高精度时间戳
        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (self.window_seconds * 1000)
        
        # 3. 【核心防御】生成绝对唯一的打卡记录 (时间戳 + UUID)
        # 防止大模型在同一毫秒内并发调用导致 Redis 哈希键碰撞覆盖
        unique_member = f"{current_time_ms}:{uuid.uuid4().hex}"
```

具体算法动作：

- **清理（Slide）：** 利用 zremrangebyscore 删除时间戳在窗口（例如过去 180 秒）之外的陈旧记录。这就相当于把窗口往前“滑动”了一下。

- **打卡（Record）：** 利用 zadd 压入当前时间的记录。这里特别用到了 UUID 与时间戳拼接，是为了解决毫秒级高并发下 ZSET 成员冲突覆盖的问题。

- **盘点（Count）：** 利用 zcard 统计当前窗口内剩余的有效调用次数。
```python
            # 4. 【性能优化】开启 Redis Pipeline 流水线
            # 把 3 条命令打包成 1 个网络包发给 Redis，将长尾延迟压制在 5ms 以内
            pipeline = self.redis_client.pipeline()
            
            # 动作 A：橡皮擦。清理时间窗口之前的陈旧数据
            pipeline.zremrangebyscore(redis_key, 0, window_start_ms)
            # 动作 B：盖章。压入本次调用的唯一记录
            pipeline.zadd(redis_key, {unique_member: current_time_ms})
            # 动作 C：盘点。统计当前窗口内还剩多少条有效记录
            pipeline.zcard(redis_key)
            
            # 5. 一次性执行流水线并获取结果
            results = pipeline.execute()
            current_count = results[2] # zcard 的结果在列表的第 3 个位置
            
            logger.debug(f"[滑动窗口] {tool_name} (会话:{session_id}) 窗口内调用量: {current_count}/{self.max_calls}")
```

工程化亮点：这三个步骤被打包进了一个 Redis pipeline() 中执行。这在算法层面保证了网络 I/O 的最小化，将三次网络往返压缩成一次，是典型的企业级高并发处理手法。


## 2. path_hash.py：基于有向拓扑与哈希降维的死循环检测算法
这是一个为了解决 LLM Agent 特有幻觉（例如在两个工具之间来回横跳：Search -> Compile -> Search -> Compile）而设计的混合型统计算法。单靠上面的频率限制无法抓出这种“交替型”死循环。

- **数据结构依赖：** 结合了 Redis 的列表（List）和有序集合（ZSET）。
```python
class PathHashLoopDetector:
    """
    企业级有向拓扑交替死循环检测器 (基于 Path-Hash)
    """
    def __init__(self, redis_client: Redis, history_length: int = 6, 
                 window_seconds: int = 180, max_path_repeats: int = 3):
        """
        初始化拓扑雷达
        :param redis_client: Redis 客户端连接
        :param history_length: 追踪最近多少步的执行轨迹，默认 6 步
        :param window_seconds: 观察滑动窗口跨度，默认 3 分钟
        :param max_path_repeats: 相同路径指纹在窗口期内允许出现的最大次数
        """
        self.redis_client = redis_client
        self.history_length = history_length
        self.window_seconds = window_seconds
        self.max_path_repeats = max_path_repeats
```


具体算法动作：

- **维护定长尾迹（Fixed-Length Trajectory）：** 使用 rpush 和 ltrim 维护一个只包含最近 N 步（默认 6 步）的队列。这就相当于在图里截取了一条固定长度的路径。
```python
            # ==========================================
            # 阶段一：维护会话的“最近 N 步飞行尾迹”
            # ==========================================
            pipe = self.redis_client.pipeline()
            # 1. 压入当前工具
            pipe.rpush(list_key, tool_name)
            # 2. 截断队列，永远只保留最近 N 步 (从 -history_length 保留到 -1)
            pipe.ltrim(list_key, -self.history_length, -1)
            # 3. 拿出当前全部轨迹
            pipe.lrange(list_key, 0, -1)
            
            list_results = pipe.execute()
            current_path = list_results[2] # 拿到 lrange 的返回列表
```

- **特征降维（Hash Fingerprinting）：** 将这几步动作拼接成字符串（如 compile ➜ search ➜ compile），然后执行 MD5 哈希计算。这一步将极长的、不规则的路径字符串压缩成了一个简短且唯一的状态指纹。
```python
    def _calculate_path_hash(self, path_list: list) -> str:
        """
        核心算法：将路径序列转换为 MD5 拓扑指纹
        输入示例: ['compile', 'search', 'compile', 'search']
        """
        # 必须凑够一定的步数才开始算作一个有效路径（比如至少3步）
        if len(path_list) < 3:
            return "too_short_to_hash"
            
        path_str = " ➜ ".join(path_list)
        # 计算 MD5，把长字符串压缩成极短的唯一指纹
        return hashlib.md5(path_str.encode('utf-8')).hexdigest()
```
```python
            # ==========================================
            # 阶段二：计算拓扑指纹
            # ==========================================
            path_hash = self._calculate_path_hash(current_path)
            
            if path_hash == "too_short_to_hash":
                return True # 步数太少，还没形成规律，安全放行
                
            logger.debug(f"[轨迹追踪] 当前路径: {' ➜ '.join(current_path)} | 指纹: {path_hash[:8]}")
```

模式频次核查（Pattern Frequency Check）：将计算出的 MD5 指纹作为 Key，再次复用上述的“滑动窗口算法”。如果同一个“路径指纹”在短时间内出现了超过允许的次数（默认 3 次），就判定为陷入了死循环逻辑。
```python
            # ==========================================
            # 阶段三：对该“指纹”执行滑动窗口限流
            # ==========================================
            hash_window_key = f"path_window:{session_id}:{path_hash}"
            
            current_time_ms = int(time.time() * 1000)
            window_start_ms = current_time_ms - (self.window_seconds * 1000)
            unique_member = f"{current_time_ms}:{uuid.uuid4().hex}"

            # 再次开启流水线，对这个特定指纹进行频次盘点
            pipe = self.redis_client.pipeline()
            pipe.zremrangebyscore(hash_window_key, 0, window_start_ms)
            pipe.zadd(hash_window_key, {unique_member: current_time_ms})
            pipe.zcard(hash_window_key)
            
            zset_results = pipe.execute()
            repeat_count = zset_results[2]
```
## 3. 执法决策
1. window.py 防的是 **“单点爆破”** 与 **“无脑重试”** 滑动窗口限流器的监控维度是单一工具。它不关心你的上下文逻辑，只盯着“某个特定的工具被调用了多少次”。
- 典型发病症状： Agent 遇到一个报错，然后像卡壳一样，连续重复调用同一个工具。
- 轨迹示例： Search -> Search -> Search -> Search
- 核心痛点： 这种行为会导致单点 API 成本剧增（比如把你的 Google Search API 配额刷爆）。
- 本质： 它是资源保护与频率控制（Rate Limiting），类似于防止用户疯狂点击同一个按钮。
```python
            # 6. 【执法决策】如果超标，立刻拔网线！
            if current_count > self.max_calls:
                logger.warning(f"[熔断警报] 拦截！会话 {session_id} 疯狂调用 {tool_name} 达 {current_count} 次！")
                
                # 抛出我们在 aspect.py 中定义的专属异常
                raise SecurityInterceptionException(
                    f"Tool Loop Detected: '{tool_name}' has been called {current_count} times "
                    f"within {self.window_seconds} seconds. Execution blocked."
                )
```
2. path_hash.py 防的才是真正的 **“逻辑绕圈子”** 有向拓扑检测器的监控维度是执行轨迹（多个工具的组合序列）。大型模型在复杂任务中最可怕的幻觉不是卡在一个工具上，而是陷入“隐蔽的交替死锁”。
- 典型发病症状： Agent 认为自己在推进任务，但实际上是在两个或多个状态之间来回横跳。
- 轨迹示例： Search -> Compile (报错) -> Fix_Code -> Compile (同样报错) -> Search -> Compile (报错)
- 核心痛点： 此时单一工具的调用频率可能并没有触发 window.py 的阈值（因为每次中间都隔了其他工具），普通的频控根本抓不住它。它会无休止地消耗 Token 且毫无进展。
- 本质： 这是真正的业务逻辑死锁拦截（Loop Detection），它通过把路径转化为 MD5 指纹来发现这种“换汤不换药”的循环。
```python
            # ==========================================
            # 阶段四：终极审判
            # ==========================================
            if repeat_count > self.max_path_repeats:
                logger.error(f"[交替死循环捕获] 会话 {session_id} 陷入逻辑死锁！路径特征: {path_hash[:8]}")
                raise SecurityInterceptionException(
                    f"Alternating Loop Detected: The execution path pattern has repeated "
                    f"{repeat_count} times. Execution violently blocked."
                )

```
| 对比维度 | window.py (滑动窗口) | path_hash.py (路径哈希) |
|---|---|---|
| 监控对象 | 单个特定的工具 | N 步工具调用组成的拓扑序列 |
| 底层标识 (Key) | tool_name | 路径字符串的 MD5 散列值 |
| 触发条件 | 同一工具在窗口期内调用次数超标 | 同一种“执行模式”在窗口期内反复出现 |
| 解决的根问题 | 单点工具 API 滥用 / 疯狂重试 | 复杂任务规划失败导致的逻辑打转 |

简单来说，window.py 是用来防“结巴”的，而 path_hash.py 是用来防“鬼打墙”的。path_hash.py 内部实际上复用了滑动窗口的统计思想，但它统计的标的物从“单个工具”升维成了“多步行为的特征指纹”。两人在防御纵深上是互补的上下游关系，缺一不可。
总结来说，window.py 解决了“单点高频爆发”的问题，而 path_hash.py 解决了“复杂逻辑闭环”的问题。它们都是拥有完整入参、状态存储、计算逻辑和决策输出的独立算法单元。
# 3. DualLayerMemoryGovernor（双层记忆管理器）
memory.py 实现的是 DualLayerMemoryGovernor（双层记忆管理器）。它的核心业务价值非常明确：解决长文本幻觉，并斩断 Token 成本二次方雪崩。

在传统的 LLM Agent 循环中，如果把每一步的执行结果无脑追加到上下文里，不仅会导致 API 费用呈指数级飙升，还会让大模型的注意力机制在冗长的历史中“失焦”。这个模块通过一套 **“主线程高保真滑动 + 后台异步语义压缩”** 的机制优雅地解决了这个问题。

以下是对其底层逻辑的客观拆解：

## 1. 架构设计：动静分离的双层记忆模型
它采用了类似计算机存储中“L1 缓存 + L2 硬盘”的分级设计：
```python
class DualLayerMemoryGovernor:
    """
    企业级双层记忆管理器 (主动滑动窗口 + 后台异步轻量化摘要)
    目标：解决长文本幻觉，斩断 Token 成本二次方雪崩。
    """
    def __init__(self, max_window_size: int = 3):
        """
        :param max_window_size: 核心高保真窗口保留的最大轮数，默认 3 轮
        """
        self.max_window_size = max_window_size
        
        # 【架构核心】独立的后台线程池
        # 记忆压缩绝不能阻塞成员 A 的主状态机流转，必须异步执行
        self.async_pool = ThreadPoolExecutor(
            max_workers=4, 
            thread_name_prefix="Memory_Summarizer"
        )
```

- **短期高保真窗口 (Active Window)：** 这是 L1 缓存。它严格限制了最大轮数（默认 max_window_size = 3）。它保存的是最近几轮交互的“原图”（包含完整的原始输入、输出、甚至详细的栈报错），确保 Agent 对眼前的局部任务具有绝对清晰的高清感知。

- **长期摘要池 (Global Summary)：** 这是 L2 硬盘。它存放的是被压缩后的“历史里程碑”字符串，体积极小，用于维持全局的任务方向感。

## 2. 主流程流转：无阻塞的 FIFO 剔除 (process_memory_tick)
每当 Agent 图状态机完成一轮推理，就会调用此方法更新记忆：
```python
    def process_memory_tick(self, active_window: list, global_summary: list, new_turn: dict) -> list:
        """
        在每一轮大模型推理结束后，由成员 A 的图状态机调用此接口。
        
        :param active_window: 当前的短期高保真窗口 (传引用)
        :param global_summary: 当前的长期摘要池 (传引用)
        :param new_turn: 本轮刚产生的新交互日志 (含入参、执行结果、报错文本等)
        :return: 更新后的 active_window
        """
        # 1. 将最新的一轮高保真记录压入短期窗口
        active_window.append(new_turn)

        # 2. 检查是否溢出 (滑动窗口机制)
        if len(active_window) > self.max_window_size:
            # 踢出最旧的一轮 (FIFO 先进先出)
            evicted_turn = active_window.pop(0)
            
            logger.debug(f"[记忆滑动] 窗口超载 (> {self.max_window_size} 轮)，剔除最旧日志并移交后台。")

            # 3. 【核心设计】将弹出的几万字老日志丢进线程池异步压缩，主进程瞬间返回
            self.async_pool.submit(
                self._generate_milestone_summary_task, 
                evicted_turn, 
                global_summary
            )

        return active_window
```

- **压入与溢出：** 新的一轮交互被追加进 active_window。一旦发现总轮数超过了设定的阈值，就会触发 FIFO（先进先出），将最旧的一轮记录（evicted_turn）弹出。

- **零阻塞移交 (核心工程亮点)：** 弹出的旧记录可能包含几万字的网页抓取结果或代码报错。这里没有选择在主进程中直接对其进行总结，而是利用独立的 ThreadPoolExecutor（最大 4 个 Worker，专有前缀 Memory_Summarizer）将其提交给后台异步处理。这确保了主进程瞬间返回，Agent 的主体流转不会因为记忆归档操作而产生任何卡顿延迟。

## 3. 后台语义压缩引擎 (_generate_milestone_summary_task)
这是被移交到后台线程池的实际执行体，它的设计充满了生产环境的防御性细节：

- **物理架构解耦：** 压缩逻辑没有强绑定任何特定的 LLM 客户端，而是通过全局注册表 registry.get("llm_router") 动态获取大模型路由实例。
```python
            # 1. 动态获取数据平面的大模型路由
            # 这里通过你们团队公共的 registry 来获取，避免物理强耦合
            from src.common.registry import registry
            llm_router = registry.get("llm_router")
            
            if not llm_router:
                raise RuntimeError("LLMRouter 未就绪，无法进行语义压缩")
```

- **强制防爆破截断：** 在将废弃的日志丢给模型总结前，代码强制对原始输出执行了 raw_output[:2000] 的切片操作。这是一种硬底线防御，防止某些极端异常（如工具输出了几百万字的乱码日志）反向把负责“总结”的模型上下文给撑爆。
```python
            # 2. 提取需要压缩的原始长文本
            tool_name = evicted_turn.get("tool_name", "unknown")
            raw_output = evicted_turn.get("output", "")
            raw_error = evicted_turn.get("error", "")
```
- **低幻觉的事实提取：** 压缩的 Prompt 严格限制了输出格式（“不超过30个字的一句话”）和内容方向（“客观总结核心结果”），并在调用大模型时强制锁定了 temperature=0.1。这是为了确保记忆池里只存入高密度的客观事实，严防大模型在总结时发散出虚假信息。
```python
            # 3. 组装提炼 Prompt
            prompt = (
                f"你是一个记忆压缩助手。Agent刚才执行了工具 [{tool_name}]。\n"
                f"执行输出：\n{raw_output[:2000]}\n"  # 限制下输入长度防爆炸
                f"报错信息：{raw_error}\n\n"
                f"请用不超过30个字的一句话，客观总结这次工具执行的核心结果（发现了什么，或失败原因是什么），作为历史里程碑记录。"
            )

            # 4. 真实调用大模型 (使用低温度保证客观事实)
            milestone_content = llm_router.chat_primary(
                messages=[{"role": "user", "content": prompt}], 
                temperature=0.1
            )
            
            milestone = f"[{tool_name} 结果]: {milestone_content.strip()}"
```

- **静默降级兜底 (Fail-Open)：** 由于这是一个异步的边缘计算任务，如果 llm_router 发生故障（如 API 超时、配额耗尽），异步线程会通过 try-except 拦截报错，并自动降级为简单的字符串拼接（"历史里程碑 -> 尝试了..."）。它保证了即使压缩服务整体挂掉，也不会导致业务主线崩溃，更不会把异常抛到前台。
```python
        except Exception as e:
            # 异步线程里的报错不能影响主进程
            logger.error(f"[后台摘要] 记忆压缩失败，降级为默认拼接: {e}")
            # Fail-Open: 如果 API 挂了，降级回退到你的字符串拼接方案
            fallback_milestone = f"历史里程碑 -> 尝试了 {evicted_turn.get('tool_name')}"
            global_summary.append(fallback_milestone)
```
总结来说， memory.py 本质上是一个带垃圾回收（GC）机制的上下文管理器。它牺牲了一部分历史记录的颗粒度，换取了极其稳定的 Token 消耗曲线和 Agent 注意力的长效聚焦。