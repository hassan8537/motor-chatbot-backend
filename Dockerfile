# Use AWS Lambda Node.js 18.x base image
FROM public.ecr.aws/lambda/nodejs:18

# Install system dependencies for OCRmyPDF and pdf processing
RUN yum update -y && \
  yum install -y \
  python3 \
  python3-pip \
  tesseract \
  tesseract-langpack-eng \
  tesseract-langpack-spa \
  tesseract-langpack-fra \
  tesseract-langpack-deu \
  ghostscript \
  qpdf \
  unpaper \
  pngquant \
  jbig2dec \
  leptonica-devel \
  poppler-utils \
  ImageMagick \
  gcc \
  gcc-c++ \
  make && \
  yum clean all && \
  rm -rf /var/cache/yum

# Install OCRmyPDF and dependencies
RUN pip3 install --no-cache-dir \
  ocrmypdf==13.7.0 \
  pillow \
  img2pdf \
  reportlab

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy package files first for better layer caching
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --only=production --no-audit --no-fund || \
  (npm cache clean --force && npm ci --only=production --no-audit --no-fund)

# Copy source code
COPY . .

# Remove development files
RUN rm -rf \
  .git \
  .gitignore \
  .dockerignore \
  README.md \
  docs/ \
  tests/ \
  *.log \
  .env.example \
  .vscode/ \
  .idea/ && \
  find . -name "*.test.js" -delete && \
  find . -name "*.spec.js" -delete

# Set environment variables
ENV TESSDATA_PREFIX=/usr/share/tesseract/tessdata/
ENV PATH="/usr/bin:/opt/bin:${PATH}"
ENV MAGICK_MEMORY_LIMIT=256MB
ENV MAGICK_MAP_LIMIT=512MB
ENV OMP_THREAD_LIMIT=1
ENV NODE_ENV=production
ENV NODE_OPTIONS="--max-old-space-size=512"

# Create necessary directories
RUN mkdir -p /tmp && chmod 755 /tmp

# Set Lambda handler
CMD ["lambda.handler"]
