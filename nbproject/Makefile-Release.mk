#
# Generated Makefile - do not edit!
#
# Edit the Makefile in the project folder instead (../Makefile). Each target
# has a -pre and a -post target defined where you can add customized code.
#
# This makefile implements configuration specific macros and targets.


# Environment
MKDIR=mkdir
CP=cp
GREP=grep
NM=nm
CCADMIN=CCadmin
RANLIB=ranlib
CC=gcc
CCC=g++
CXX=g++
FC=gfortran
AS=as

# Macros
CND_PLATFORM=GNU-Linux
CND_DLIB_EXT=so
CND_CONF=Release
CND_DISTDIR=dist
CND_BUILDDIR=build

# Include project Makefile
include Makefile

# Object Directory
OBJECTDIR=${CND_BUILDDIR}/${CND_CONF}/${CND_PLATFORM}

# Object Files
OBJECTFILES= \
	${OBJECTDIR}/_ext/a0c56a08/FsMutationsTableTailer.o \
	${OBJECTDIR}/_ext/a0c56a08/Notifier.o \
	${OBJECTDIR}/_ext/a0c56a08/TableTailer.o \
	${OBJECTDIR}/_ext/a0c56a08/main.o


# C Compiler Flags
CFLAGS=

# CC Compiler Flags
CCFLAGS=-DBOOST_LOG_DYN_LINK
CXXFLAGS=-DBOOST_LOG_DYN_LINK

# Fortran Compiler Flags
FFLAGS=

# Assembler Flags
ASFLAGS=

# Link Libraries and Options
LDLIBSOPTIONS=-L/usr/local/mysql/lib -lndbclient -lboost_system -lcppnetlib-client-connections -lboost_thread -lpthread -lcppnetlib-uri -lboost_log -lboost_log_setup -lboost_date_time -lboost_program_options

# Build Targets
.build-conf: ${BUILD_SUBPROJECTS}
	"${MAKE}"  -f nbproject/Makefile-${CND_CONF}.mk ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/hopsfs-elastic-notifier

${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/hopsfs-elastic-notifier: ${OBJECTFILES}
	${MKDIR} -p ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}
	${LINK.cc} -o ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/hopsfs-elastic-notifier ${OBJECTFILES} ${LDLIBSOPTIONS}

${OBJECTDIR}/_ext/a0c56a08/FsMutationsTableTailer.o: /home/maism/src/hopsfs-elastic-notifier/FsMutationsTableTailer.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/a0c56a08
	${RM} "$@.d"
	$(COMPILE.cc) -O2 -I/usr/local/mysql/include -I/usr/local/mysql/include/storage/ndb -I/usr/local/mysql/include/storage/ndb/ndbapi -I/usr/local/mysql/include/storage/ndb/mgmapi -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/a0c56a08/FsMutationsTableTailer.o /home/maism/src/hopsfs-elastic-notifier/FsMutationsTableTailer.cpp

${OBJECTDIR}/_ext/a0c56a08/Notifier.o: /home/maism/src/hopsfs-elastic-notifier/Notifier.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/a0c56a08
	${RM} "$@.d"
	$(COMPILE.cc) -O2 -I/usr/local/mysql/include -I/usr/local/mysql/include/storage/ndb -I/usr/local/mysql/include/storage/ndb/ndbapi -I/usr/local/mysql/include/storage/ndb/mgmapi -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/a0c56a08/Notifier.o /home/maism/src/hopsfs-elastic-notifier/Notifier.cpp

${OBJECTDIR}/_ext/a0c56a08/TableTailer.o: /home/maism/src/hopsfs-elastic-notifier/TableTailer.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/a0c56a08
	${RM} "$@.d"
	$(COMPILE.cc) -O2 -I/usr/local/mysql/include -I/usr/local/mysql/include/storage/ndb -I/usr/local/mysql/include/storage/ndb/ndbapi -I/usr/local/mysql/include/storage/ndb/mgmapi -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/a0c56a08/TableTailer.o /home/maism/src/hopsfs-elastic-notifier/TableTailer.cpp

${OBJECTDIR}/_ext/a0c56a08/main.o: /home/maism/src/hopsfs-elastic-notifier/main.cpp 
	${MKDIR} -p ${OBJECTDIR}/_ext/a0c56a08
	${RM} "$@.d"
	$(COMPILE.cc) -O2 -I/usr/local/mysql/include -I/usr/local/mysql/include/storage/ndb -I/usr/local/mysql/include/storage/ndb/ndbapi -I/usr/local/mysql/include/storage/ndb/mgmapi -MMD -MP -MF "$@.d" -o ${OBJECTDIR}/_ext/a0c56a08/main.o /home/maism/src/hopsfs-elastic-notifier/main.cpp

# Subprojects
.build-subprojects:

# Clean Targets
.clean-conf: ${CLEAN_SUBPROJECTS}
	${RM} -r ${CND_BUILDDIR}/${CND_CONF}
	${RM} ${CND_DISTDIR}/${CND_CONF}/${CND_PLATFORM}/hopsfs-elastic-notifier

# Subprojects
.clean-subprojects:

# Enable dependency checking
.dep.inc: .depcheck-impl

include .dep.inc
