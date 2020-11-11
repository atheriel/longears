AC_INIT([longears])

AC_LANG(C)
AC_REQUIRE_CPP
AC_PROG_CC

dnl Grab R's C flags and compiler.
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
CC=`"${R_HOME}/bin/R" CMD config CC`
CFLAGS=`"${R_HOME}/bin/R" CMD config CFLAGS`

# Look for librabbitmq/rabbitmq-c.
have_librabbitmq=no

AC_SEARCH_LIBS(amqp_new_connection, rabbitmq, [have_librabbitmq=yes])
AC_CHECK_HEADERS(amqp.h, [have_librabbitmq=yes])

if test "x${have_librabbitmq}" = xno; then
  AC_MSG_FAILURE([
---
  The librabbitmq/rabbitmq-c headers could not be found. Please install

    - librabbitmq-dev on Debian-based systems (including Ubuntu)
    - librabbitmq-devel on Fedora-based systems (including CentOS, RHEL)
    - librabbitmq-c on Arch-based systems
    - rabbitmq-c (via Homebrew) on macOS
    - librabbitmq (via vcpkg) on Windows

  and try again. If you do have this library and this installation still fails,
  you can provide the location of the library directly with

  R CMD INSTALL longears --configure-vars='LIBS=-L/path/to/libs CPPFLAGS=-I/path/to/headers'
---])
fi

AC_SUBST([PKG_LIBS],["${PKG_LIBS} ${ac_cv_search_amqp_new_connection}"])
AC_SUBST([PKG_CPPFLAGS],["${PKG_CPPFLAGS}"])
AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT

echo "---
  Configuration:

  CC:        ${CC}
  CFLAGS:    ${CFLAGS}
  CPPFLAGS:  ${PKG_CPPFLAGS}
  LIBS:      ${PKG_LIBS}
---"
