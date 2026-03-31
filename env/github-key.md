# GitHub SSH 双账号配置与推送

## 1. 生成两把密钥（私人/公司）

```bash
ssh-keygen -t ed25519 -C "SinnHuu@outlook.com" -f ~/.ssh/id_ed25519_github_personal
ssh-keygen -t ed25519 -C "sinn@echooo.co" -f ~/.ssh/id_ed25519_github_work
```

## 2. 启动 Agent 并加载密钥

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519_github_personal
ssh-add ~/.ssh/id_ed25519_github_work
```

## 3. 把公钥分别加到两个 GitHub 账号

```bash
cat ~/.ssh/id_ed25519_github_personal.pub
cat ~/.ssh/id_ed25519_github_work.pub
```

去 GitHub 的 `Settings -> SSH and GPG keys`：  
私人账号加 `personal` 公钥，公司账号加 `work` 公钥。

## 4. 配置 `~/.ssh/config`

```sshconfig
Host github-personal
HostName github.com
User git
IdentityFile ~/.ssh/id_ed25519_github_personal
IdentitiesOnly yes

Host github-work
HostName github.com
User git
IdentityFile ~/.ssh/id_ed25519_github_work
IdentitiesOnly yes
```

## 5. 测试是否对应到正确账号

```bash
ssh -T git@github-personal
ssh -T git@github-work
```

## 6. 给仓库设置对应 Remote（关键）

私人仓库：

```bash
git remote set-url origin git@github-personal:SinnHuu1/xxx.git
```

公司仓库：

```bash
cd ~/Documents/Work/data-warehouse
git remote set-url origin git@github-work:HEchooo/data-warehouse.git
```

这样以后在不同仓库里 `git push` 会自动使用对应账号，不需要手动切换登录状态。

## 7. 推送

1. 验证连接

```bash
cd ~/Documents/Work/data-warehouse
ssh -T git@github-work
```

2. 推送到远程 `main`（第一次推这个分支用 `-u`）

```bash
git push -u origin main
```

3. 后续再推同一分支只要

```bash
git push
```
