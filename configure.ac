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

AC_SEARCH_LIBS(amqp_new_connection, rabbitmq, [], [AC_ERROR([librabbitmq is required. Try 'apt install librabbitmq-dev'.])])
AC_CHECK_HEADERS(amqp.h, [], [AC_ERROR([The librabbitmq headers are required. Try 'apt install librabbitmq-dev'.])])

AC_SUBST([PKG_LIBS],["${PKG_LIBS} ${ac_cv_search_amqp_new_connection}"])
AC_SUBST([PKG_CPPFLAGS],["${PKG_CPPFLAGS}"])
AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT

echo "
Configuration:
==============

cc:      ${CC}
cflags:  ${CFLAGS}
libs:    ${PKG_LIBS}
"
