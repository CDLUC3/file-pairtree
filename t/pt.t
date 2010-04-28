use 5.006;
use Test::More qw( no_plan );
use strict;
use warnings;

my $script = 'pt';		# script we're testing

# as of 2010.04.28  (SHELL stuff, remake_td, Config perlpath minus _exe)
#### start boilerplate for script name and temporary directory support

use Config;
$ENV{SHELL} = "/bin/sh";
my $td = "td_$script";		# temporary test directory named for script
# Depending on circs, use blib, but prepare to use lib as fallback.
my $blib = (-e "blib" || -e "../blib" ?	"-Mblib" : "-Ilib");
my $bin = ($blib eq "-Mblib" ?		# path to testable script
	"blib/script/" : "") . $script;
my $perl = $Config{perlpath};		# perl used in testing
my $cmd = "2>&1 $perl $blib " .		# command to run, capturing stderr
	(-x $bin ? $bin : "../$bin") . " ";	# exit status in $? >> 8

my ($rawstatus, $status);		# "shell status" version of "is"
sub shellst_is { my( $expected, $output, $label )=@_;
	$status = ($rawstatus = $?) >> 8;
	$status != $expected and	# if not what we thought, then we're
		print $output, "\n";	# likely interested in seeing output
	return is($status, $expected, $label);
}

use File::Path;
sub remake_td {		# make $td with possible cleanup
	-e $td			and remove_td();
	mkdir($td)		or die "$td: couldn't mkdir: $!";
}
sub remove_td {		# remove $td but make sure $td isn't set to "."
	! $td || $td eq "."	and die "bad dirname \$td=$td";
	eval { rmtree($td); };
	$@			and die "$td: couldn't remove: $@";
}

#### end boilerplate

{
remake_td();
my $x;

$x = `$cmd -d $td mknode abc`;
is $?, 0, "status good on simple mknode";

like $x, qr|ab/c/|, "simple mknode";

$x = `$cmd -d $td lstree`;
is $?, 0, "status good on simple lstree";

like $x, qr|abc\n1 object\s*$|, "simple lstree with one node";

remake_td();		# re-make temp dir

$x = `$cmd -d dummy mktree $td prefix`;
is $?, 0, "status good on mktree with prefix and ignored -d";

$x = `$cmd -d dummy lstree`;
isnt $?, 0, "status non-zero on lstree as -d wasn't created";
chop $x;

like $x, qr|no such file or dir|, "complaint of non-existent tree";

$x = `$cmd -d $td mknode prefixabc prefixdef prefixhigk`;
like $x, qr|abc.*def.*higk.$|s, "make 3 nodes at once, prefix stripped";

$x = `$cmd --dummy lsnode prefixdef`;
ok($? > 1, "status greater than 1 on bad option");

$x = `$cmd -d $td lsnode prefixxyz prefixdef`;
is $?>>8, 1, "status 1 on lsnode with at least one non-existent node";

$x = `$cmd -fd $td lsnode def`;
is $?>>8, 2, "status 2 on lsnode of existing node, no prefix, but --force)";

$x = `$cmd -d $td lsnode def`;
is $?, 0, "status good on lsnode of existing node, no prefix (no --force)";

$x = `$cmd -d $td lsnode prefixdef`;
is $?, 0, "status good on lsnode of existing node (with prefix)";

$x = `$cmd -d $td rmnode prefixdef`;
is $?, 0, "status good on rmnode of existing node (with prefix)";

$x = `$cmd -d $td rmnode prefixdummy`;
is $?>>8, 1, "soft fail on rmnode of non-existing node (with prefix)";

remake_td();		# re-make temp dir

$x = `$cmd -d $td mknode abc abcd abcde def ghi jkl`;
$x = `$cmd -d $td lstree`;
like $x, qr/6 objects/, 'make and list 6 object tree, overlapping ids';

my $R = 'pairtree_root';
`date > $td/$R/ab/c/foo`;	# set up unencapsulated file error
`mkdir $td/$R/de/f/fo`;		# set up shorty after morty error
`mkdir $td/$R/gh/i/ghi2`;	# set up unencapsulated group error

$x = `$cmd -d $td lstree`;
like $x, qr/split end/s, 'detected split end';

like $x, qr/forced path ending/s, 'detected shorty after morty';

like $x, qr/unencapsulated file/s, 'detected unencapsulated group';

#print "x=$x\n";

remove_td();
}

#done_testing();
