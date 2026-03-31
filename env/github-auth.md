# GitHub SSH 配置

适用场景：

- 本地同时连接个人 GitHub 和公司 GitHub
- 远程服务器通过公司 GitHub 拉取私有仓库

## 本地双账号

生成两把 key：

```bash
ssh-keygen -t ed25519 -C "your-personal-email" -f ~/.ssh/id_ed25519_github_personal
ssh-keygen -t ed25519 -C "your-work-email" -f ~/.ssh/id_ed25519_github_work
```

把公钥加到 GitHub：

```bash
cat ~/.ssh/id_ed25519_github_personal.pub
cat ~/.ssh/id_ed25519_github_work.pub
```

- 个人账号：`Settings -> SSH and GPG keys`
- 公司账号：`Settings -> SSH and GPG keys`

配置 `~/.ssh/config`：

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

验证账号映射：

```bash
ssh -T git@github-personal
ssh -T git@github-work
```

首次连接如果提示主机指纹，输入 `yes` 即可。

给仓库设置正确 remote：

个人仓库：

```bash
git remote set-url origin git@github-personal:<personal-org-or-user>/<repo>.git
```

公司仓库：

```bash
git remote set-url origin git@github-work:<work-org-or-user>/<repo>.git
```

```bash
git remote -v
```

## 远程服务器连接公司 GitHub

在服务器生成公司 key：

```bash
ssh-keygen -t ed25519 -C "your-work-email" -f ~/.ssh/id_ed25519_github_company
```

把公钥加到公司 GitHub：

```bash
cat ~/.ssh/id_ed25519_github_company.pub
```

- 加到公司账号：`Settings -> SSH and GPG keys`
- 加到目标仓库：`Settings -> Deploy keys`

如果服务器只拉一个仓库，优先用 `Deploy key`。

配置服务器的 `~/.ssh/config`：

```sshconfig
Host github-company
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519_github_company
  IdentitiesOnly yes
```

测试 SSH 登录：

```bash
ssh -T git@github-company
```

看到下面这类输出，说明认证成功：

```text
Hi <account>! You've successfully authenticated, but GitHub does not provide shell access.
```

把仓库 remote 从 HTTPS 改成 SSH：

```bash
git remote -v
```

```bash
git remote set-url origin git@github-company:<owner>/<repo>.git
git remote -v
```

拉取代码：

```bash
git pull
```

如果不想每次看到 pull 策略提示：

```bash
git config pull.rebase false
```

## 常见问题

`Repository not found` 常见原因：

- 当前 remote 还是 `https://github.com/...`
- SSH key 已认证成功，但对应账号没有该私有仓库权限
- 仓库已经迁移到别的组织或账号，remote 地址仍是旧地址

`ssh -T` 成功但 `git pull` 失败：

- key 加到了正确的 GitHub 账号或正确的仓库
- `git remote -v` 指向的是正确仓库地址
