FROM rhub/r-minimal:4.0.5

# Prep the image
RUN apk update

RUN apk add --no-cache --update-cache \
    --repository http://nl.alpinelinux.org/alpine/v3.11/main \
    autoconf=2.69-r2 \
    automake=1.16.1-r0 \
    bash tzdata

RUN echo "America/New York" > /etc/timezone

# Install system and R packages
# -d Remove compilers after usage
# -t Remove these build-time system packages after R packages have been compiled and installed
# -a These are run-time system packages
# 
RUN installr -d \
    -t "R-dev file linux-headers libxml2-dev curl-dev openssl-dev fontconfig-dev harfbuzz-dev fribidi-dev freetype-dev libpng-dev tiff-dev jpeg-dev mariadb-dev" \
    -a "libxml2 mariadb-client curl openssl fontconfig harfbuzz fribidi freetype libpng tiff libjpeg icu mariadb-connector-c" \
    tidyverse lubridate tidyr DBI RMySQL janitor jsonlite dotenv

RUN rm -rf /var/cache/apk/*

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

COPY etl_data_pipeline.R .
COPY my.cnf .
COPY us-west-2-bundle.pem .

RUN chown app:app -R /app

USER app
CMD Rscript etl_data_pipeline.R;
