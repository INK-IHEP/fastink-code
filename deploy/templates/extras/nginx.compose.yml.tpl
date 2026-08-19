services:
  fastink-nginx:
    image: nginx:1.27-alpine
    restart: unless-stopped
    depends_on:
      fastink-server:
        condition: service_healthy
    volumes:
      - {{ nginx_conf_path }}:/etc/nginx/conf.d/default.conf:ro
      - {{ nginx_conf_dir }}/servers/:/etc/nginx/conf.d/ink-servers/:ro
      - {{ nginx_conf_dir }}/snippets/:/etc/nginx/conf.d/ink-snippets/:ro
      - {{ nginx_conf_dir }}/locations/:/etc/nginx/conf.d/ink-locations/:ro
      - {{ nginx_conf_dir }}/locations-overlay/:/etc/nginx/conf.d/ink-locations-overlay/:ro
      - {{ nginx_cert_host_path }}:{{ nginx_cert_container_path }}:ro
      - {{ nginx_key_host_path }}:{{ nginx_key_container_path }}:ro
    ports:
      - "{{ host_port }}:443"
