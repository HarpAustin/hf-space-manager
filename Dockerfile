# 使用官方 Python 镜像作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 将当前目录下的所有文件复制到工作目录
COPY . .

# 安装项目依赖
RUN pip install Flask python-dotenv huggingface_hub requests gunicorn flask-socketio python-engineio python-socketio eventlet apscheduler

# 开放应用程序的端口
EXPOSE 5000

# 设置环境变量（可选，如果需要传递 Docker 环境中的环境变量）
# ENV USERNAME=your_username
# ENV PASSWORD=your_password
# ENV HF_TOKENS=token1,token2,token3
# ENV API_KEY=your_apikey

# 定义启动命令
CMD ["gunicorn", "--worker-class", "eventlet", "--bind", "0.0.0.0:5000", "app:app"]
