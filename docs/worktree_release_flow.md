# 工作区分离与发布流

## 当前结构

现在已经拆成两份代码工作区：

- 开发工作区：`C:\Users\Maru\Documents\agent`
- MVP 工作区：`C:\Users\Maru\Documents\agent_mvp`

对应分支：

- 开发分支：`codex/dev-local`
- MVP 分支：`codex/mvp-live`

这两份目录是独立 checkout：

- 你在开发目录改代码，不会直接污染 MVP 目录
- 你在 MVP 目录验证或启动服务，也不会影响开发目录里的未完成改动

## 推荐日常动作

### 1. 开发

在开发工作区里工作：

```powershell
cd C:\Users\Maru\Documents\agent
```

启动开发环境：

```powershell
scripts\start-demo-dev.cmd
```

### 2. MVP 对外

在 MVP 工作区里运行：

```powershell
cd C:\Users\Maru\Documents\agent_mvp
```

启动 MVP：

```powershell
scripts\start-demo-mvp.cmd
```

如果要穿透：

```powershell
scripts\start-demo-mvp-ngrok.cmd
```

## 后面怎么“合并”

以后你真正要做的不是“合并环境”，而是把开发分支的成果发布到 MVP 分支。

标准做法：

1. 在开发工作区完成修改
2. 自测通过
3. 提交开发分支
4. 进入 MVP 工作区
5. 把需要的提交合入 `codex/mvp-live`
6. 重启 MVP 服务

## 两种发布方式

### 方式 A：最稳，按提交发布

开发工作区：

```powershell
cd C:\Users\Maru\Documents\agent
git status
git add .
git commit -m "你的说明"
```

MVP 工作区：

```powershell
cd C:\Users\Maru\Documents\agent_mvp
git merge codex/dev-local
```

如果你只想挑一部分提交，也可以：

```powershell
git cherry-pick <commit_sha>
```

### 方式 B：今天赶时间，先冻结快照

如果当前还没来得及整理提交，可以先把开发目录里的当前改动同步成 MVP 快照，再用 MVP 工作区启动服务。

这种做法适合今天下午先开放 MVP，但后面还是建议尽快回到“按提交发布”的方式。

可以直接在开发工作区运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync-dev-snapshot-to-mvp.ps1
```

## 当前阶段建议

今天建议你这样跑：

1. `agent` 继续作为开发目录
2. `agent_mvp` 作为对外目录
3. 下午对外只启动 `agent_mvp`
4. 后续所有新开发都优先在 `agent` 进行
5. 每次准备发布时，把稳定改动合到 `codex/mvp-live`

## 一个重要边界

如果开发目录和 MVP 目录同时运行服务，请一定记住：

- `agent` 跑 `dev`
- `agent_mvp` 跑 `mvp`

不要交叉启动。

否则虽然代码目录已经分开，但你还是可能因为误点脚本把服务跑混。
