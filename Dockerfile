# Docker descriptor for online_bank
# License - http://www.eclipse.org/legal/epl-v20.html

FROM ghcr.io/codbex/codbex-gaia:0.26.0

COPY kinder-map target/dirigible/repository/root/registry/public/kinder-map

ENV DIRIGIBLE_HOME_URL=/services/web/kinder-map/gen/kinder-map/index.html

ENV DIRIGIBLE_MULTY_TENANT_MODE=false

EXPOSE 8080
