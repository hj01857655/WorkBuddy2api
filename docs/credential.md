# WorkBuddy2api 凭据

- 双 realm：国内版（已实测）与国际版（`login --realm global`）。
- 凭据落盘：`<状态目录>/` authStore（0600）。
- 每日签到：启动时自动执行（internal/checkin，authStore 原子写）。
- 8377 若被带鉴权的实例占用，需 key（该实例的 API_KEY）。
