<?php #-*-tab-width: 4; fill-column: 76; indent-tabs-mode: t -*-
# vi:shiftwidth=4 tabstop=4 textwidth=76

/*
 * Copyright (C) 2019  NicheWork, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Mark A. Hershberger <mah@nichework.com>
 */
namespace MediaWiki\Extension\PerconaDB;

use Wikimedia\RDBMS\Database as BaseDB;
use Wikimedia\RDBMS\DatabaseMysqli;

class Database extends DatabaseMysqli {
	public function lockIsFree( $lockName, $method ) {
		return BaseDB::lockIsFree( $lockName, $method );
	}

	public function lock( $lockName, $method, $timeout = 5 ) {
		return BaseDB::lock( $lockName, $method, $timeout );
	}

	public function unlock( $lockName, $method ) {
		return BaseDB::unlock( $lockName, $method );
	}

	public function namedLocksEnqueue() {
		return BaseDB::namedLocksEnqueue();
	}

	protected function doLockTables( array $read, array $write, $method ) {
		return BaseDB::doLockTables( $read, $write, $method );
	}

	protected function doUnlockTables( $method ) {
		return BaseDB::doUnlockTables( $method );
	}
}
