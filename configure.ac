AC_INIT([longears])

AC_LANG(C)
AC_REQUIRE_CPP
AC_PROG_CC

AC_ARG_WITH([librabbitmq_src],
            AC_HELP_STRING([--with-librabbitmq-src=URL],
                           [URL override for librabbitmq sources (if required)]),
            [LIBRABBITMQ_SRC_URL="${with_librabbitmq_src}"],
            [LIBRABBITMQ_SRC_URL=https://github.com/alanxz/rabbitmq-c/archive/refs/tags/v0.11.0.tar.gz])

dnl Grab R's C flags and compiler.
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
CC=`"${R_HOME}/bin/R" CMD config CC`
CFLAGS=`"${R_HOME}/bin/R" CMD config CFLAGS`

uname=`uname`

# Look for librabbitmq/rabbitmq-c.
have_librabbitmq=no

AC_SEARCH_LIBS(amqp_new_connection, rabbitmq, [have_librabbitmq=yes])
AC_CHECK_HEADERS(amqp.h, [have_librabbitmq=yes])

if test "x${have_librabbitmq}" = xyes; then
  AC_SUBST([PKG_LIBS],["${PKG_LIBS} ${ac_cv_search_amqp_new_connection}"])
  AC_SUBST([PKG_CPPFLAGS],["${PKG_CPPFLAGS}"])
  AC_SUBST([BUNDLED_LIBRABBITMQ],[""])
  AC_SUBST([CLEAN_BUNDLED_LIBRABBITMQ],[""])
fi

# Attempt to build librabbitmq from source if missing.
if test "x${have_librabbitmq}" = xno; then
  AC_MSG_NOTICE([no local librabbitmq found; attempting to build one from sources])

  # We need cmake to configure librabbitmq.
  AC_DEFUN([AC_PROG_CMAKE], [AC_CHECK_PROG(have_cmake,cmake,yes)])
  AC_PROG_CMAKE
  if test x"${have_cmake}" != x"yes"; then
    AC_MSG_ERROR(['cmake' is required to build librabbitmq from source])
  fi

  # Check for existing source files and download them if not present.
  mkdir -p src/librabbitmq/build
  AC_CHECK_FILE(src/librabbitmq/README.md, [have_librabbitmq_src=yes],
                [have_librabbitmq_src=no])
  if test x"${have_librabbitmq_src}" = x"no"; then
    AC_CHECK_FILE(inst/librabbitmq.tar.gz, [have_cached_librabbitmq_src=yes],
                  [have_cached_librabbitmq_src=no])
    if test x"${have_cached_librabbitmq_src}" = x"no"; then
      AC_MSG_NOTICE([downloading librabbitmq sources from ${LIBRABBITMQ_SRC_URL}])
      mkdir -p inst
      ${R_HOME}/bin/Rscript -e "download.file(\"$LIBRABBITMQ_SRC_URL\", \"inst/librabbitmq.tar.gz\", quiet = TRUE)"
    fi
    AC_MSG_NOTICE([extracting librabbitmq sources to src/librabbitmq])

    # Work around Solaris not supporting various tar flags.
    if test x"${uname}" = x"SunOS"; then
      gtar xzf inst/librabbitmq.tar.gz -C src/librabbitmq --strip-components 1
    else
      tar xzf inst/librabbitmq.tar.gz -C src/librabbitmq --strip-components 1
    fi
  fi

  # Check again for source files to confirm they are what we expect.
  AC_CHECK_FILE(src/librabbitmq/CMakeLists.txt, [],
                AC_MSG_FAILURE([unexpected librabbitmq source layout]))

  AC_MSG_NOTICE([configuring librabbitmq via cmake])
  cd src/librabbitmq/build
  # Build a static library, there is no point sharing in this context.
  cmake -G 'Unix Makefiles' \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_STATIC_LIBS=ON \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_SSL_SUPPORT=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TOOLS=OFF \
    -DBUILD_TESTS=OFF \
    -DBUILD_API_DOCS=OFF \
    ..
  if test "$?" != "0"; then
    AC_MSG_WARN([failed to configure librabbitmq])
  else
    have_librabbitmq=yes
    dnl AC_SUBST([PKG_LIBS],["${PKG_LIBS} -Llibrabbitmq/build/librabbitmq -lrabbitmq"])
    AC_SUBST([PKG_LIBS],["${PKG_LIBS} -L. -lrabbitmq"])
    AC_SUBST([PKG_CPPFLAGS],["${PKG_CPPFLAGS} -Ilibrabbitmq/librabbitmq"])
    AC_SUBST([BUNDLED_LIBRABBITMQ],["librabbitmq"])
    AC_SUBST([CLEAN_BUNDLED_LIBRABBITMQ],["clean-librabbitmq"])
  fi
  cd ../../..
fi

if test "x${have_librabbitmq}" = xno; then
  AC_MSG_FAILURE([
---
  The librabbitmq/rabbitmq-c could not be found or built from source. Please install

    - librabbitmq-dev on Debian-based systems (including Ubuntu)
    - librabbitmq-devel on Fedora-based systems (including CentOS, RHEL)
    - librabbitmq-c on Arch-based systems
    - rabbitmq-c (via Homebrew) on macOS
    - rabbitmq-c (via Rtools) on Windows

  and try again. If you do have this library and this installation still fails,
  you can provide the location of the library directly with

  R CMD INSTALL longears --configure-vars='LIBS=-L/path/to/libs CPPFLAGS=-I/path/to/headers'
---])
fi

AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT

echo "---
  Configuration:

  CC:        ${CC}
  CFLAGS:    ${CFLAGS}
  CPPFLAGS:  ${PKG_CPPFLAGS}
  LIBS:      ${PKG_LIBS}
---"
