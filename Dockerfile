# Use AWS Lambda Node.js 18.x base image
FROM public.ecr.aws/lambda/nodejs:18

# Set working directory
WORKDIR /var/task

# Copy and install dependencies
COPY package*.json ./
RUN npm install || npm cache clean --force && npm install

# Copy source code
COPY . .

# Set Lambda handler (exported as `exports.handler = ...`)
CMD ["lambda.handler"]
