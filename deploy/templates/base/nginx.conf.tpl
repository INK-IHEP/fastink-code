include /etc/nginx/conf.d/ink-servers/*.conf;

server {
    listen 443 ssl;
    server_name _;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    client_max_body_size 20000M;

    include /etc/nginx/conf.d/ink-snippets/*.conf;

    resolver {{ nginx_resolver }} valid=300s;

    include /etc/nginx/conf.d/ink-locations/*.conf;
    include /etc/nginx/conf.d/ink-locations-overlay/*.conf;
}
