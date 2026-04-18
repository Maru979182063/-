# 环境分离说明

## 目标

当前仓库已经拆成两套运行环境：

- `mvp`：今天下午对外开放使用的稳定环境
- `dev`：你本地持续开发和验证的环境

两套环境共用**同一份代码仓库**，但使用**不同端口、不同数据库、不同运行配置**，因此可以同时存在，不会互相覆盖运行数据。

## 当前隔离项

### 1. 启动入口

- `scripts\start-demo.cmd`
  - 默认启动 `mvp`
- `scripts\start-demo-with-ngrok.cmd`
  - 默认启动 `mvp + ngrok`
- `scripts\start-demo-mvp.cmd`
  - 显式启动 `mvp`
- `scripts\start-demo-mvp-ngrok.cmd`
  - 显式启动 `mvp + ngrok`
- `scripts\start-demo-dev.cmd`
  - 启动 `dev`，并开启 `--reload`

### 2. 端口

- `mvp`
  - `prompt_skeleton_service`: `8011`
  - `passage_service`: `8001`
- `dev`
  - `prompt_skeleton_service`: `8111`
  - `passage_service`: `8101`

### 3. 数据库

- `mvp`
  - 题目库：`prompt_skeleton_service/data/question_workbench.mvp.db`
  - 材料库：`passage_service/passage_service.mvp.db`
- `dev`
  - 题目库：`prompt_skeleton_service/data/question_workbench.dev.db`
  - 材料库：`passage_service/passage_service.dev.db`

### 4. 配置文件

- `mvp`
  - 根环境覆盖：`.env.mvp`
  - 材料服务覆盖：`passage_service/.env.mvp`
  - 运行配置：`prompt_skeleton_service/configs/question_runtime.mvp.yaml`
- `dev`
  - 根环境覆盖：`.env.dev`
  - 材料服务覆盖：`passage_service/.env.dev`
  - 运行配置：`prompt_skeleton_service/configs/question_runtime.dev.yaml`

## 这不是什么“分叉代码”

这里分离的是**运行环境**，不是两套长期分叉的代码。

你的日常节奏应该是：

1. 用 `dev` 跑新功能
2. 在 `dev` 验证通过
3. 保留代码改动不变
4. 重启 `mvp` 环境，让它加载这份最新代码

所以后面所谓“合并”，本质上不是合并环境，而是：

- 代码改完后，重新发布到 `mvp`
- `mvp` 继续使用它自己的库和配置

## 推荐使用方式

### 开发时

运行：

```bat
scripts\start-demo-dev.cmd
```

用途：

- 改代码
- 本地联调
- 看热更新
- 验证新逻辑

### 对外演示 / MVP 使用时

运行：

```bat
scripts\start-demo-mvp.cmd
```

如需公网映射：

```bat
scripts\start-demo-mvp-ngrok.cmd
```

## 发布动作

当你在 `dev` 把某个功能做完后，建议按这个顺序切换到 `mvp`：

1. 先停掉 `mvp`
2. 保留当前仓库代码改动
3. 用 `dev` 再做一次关键链路验证
4. 启动 `scripts\start-demo-mvp.cmd`
5. 冒烟验证 `mvp` 是否正常

如果后面你开始正式使用 Git 分支，那么流程会更稳：

1. 在开发分支做改动
2. 验证通过后合并到主分支
3. 用主分支启动 `mvp`

但就当前阶段来说，即使你先不引入完整分支流转，这套环境分离也已经足够支撑：

- 下午先开放 `mvp`
- 你本地继续用 `dev` 开发
- 两边互不踩库、不抢端口

## 当前最重要的边界

不要混用启动脚本。

建议你记住：

- 对外就用 `mvp`
- 开发就用 `dev`

只要入口不混，后面就不会再出现“我到底改的是哪套环境”的问题。
