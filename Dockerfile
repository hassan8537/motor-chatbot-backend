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
  make

# Install OCRmyPDF and dependencies
RUN pip3 install --no-cache-dir \
  ocrmypdf==15.4.4 \
  pillow \
  img2pdf \
  reportlab

# Set working directory
WORKDIR /var/task

# Copy and install dependencies
COPY package*.json ./
RUN npm install || npm cache clean --force && npm install

# Copy source code
COPY . .

# Set environment variables for OCR
ENV TESSDATA_PREFIX=/usr/share/tesseract/tessdata/
ENV PATH="/opt/bin:${PATH}"
ENV MAGICK_MEMORY_LIMIT=256MB
ENV MAGICK_MAP_LIMIT=512MB
ENV OMP_THREAD_LIMIT=1

# Set Lambda handler (exported as `exports.handler = ...`)
CMD ["lambda.handler"]
