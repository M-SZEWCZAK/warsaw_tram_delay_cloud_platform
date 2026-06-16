FROM nginx:alpine
COPY nginx.conf /etc/nginx/configure.conf
COPY index.html /usr/share/nginx/html/index.html
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;", "-c", "/etc/nginx/configure.conf"]
