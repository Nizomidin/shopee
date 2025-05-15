FROM public.ecr.aws/lambda/nodejs:18

# Install dependencies
RUN yum update -y
RUN yum install -y \
    dbus-libs \
    atk \
    at-spi2-core \
    libX11 \
    libXcomposite \
    libXdamage \
    libXext \
    libXfixes \
    libXrandr \
    mesa-libgbm \
    libxcb \
    libxkbcommon \
    systemd-libs \
    alsa-lib
COPY package*.json ./
RUN npm install
RUN npx playwright install
# Copy function code
COPY app.js ${LAMBDA_TASK_ROOT}
COPY .env ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]