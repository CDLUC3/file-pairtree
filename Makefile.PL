use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    EXE_FILES           => [ 'pt' ],
    NAME                => 'File::Pairtree',
    AUTHOR              => 'John A. Kunze <jak@ucop.edu>',
    VERSION_FROM        => 'VERSION',
    ABSTRACT_FROM       => 'lib/File/Pairtree.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'Apache2')
      : ()),
    PM			=> {
	'lib/File/Pairtree.pm'	=> 'blib/lib/File/Pairtree.pm'
    },
    PL_FILES            => {},
    PREREQ_PM         => {
	'Carp'		=> '0',
	'Pod::Usage'	=> '0',
	'Getopt::Long'	=> '0',
	'File::Find'	=> '0',
	'File::Path'	=> '0',
	'File::Glob'	=> '0',
	'File::ANVL'	=> '0.16',
	'File::Value'	=> '0.17',
	'File::Namaste'	=> '0.17',
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'File-Value-*' },
);
