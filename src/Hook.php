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

use DatabaseUpdater;

class Hook {
	static public function onLoadExtensionSchemaUpdates(
		DatabaseUpdater $upd
	) {
		$upd->addExtensionUpdate( [ __CLASS__ . '::ensureNoMyISAMTables' ] );
		$upd->addExtensionUpdate( [ __CLASS__ . '::ensurePrimaryKeys' ] );
	}

	/**
	 * Return an array of tables without a primary key and the type of key
	 * (UNI, MUL) used for each column.  This information is used later to
	 * construct a primary key.
	 *
	 * @param Database $dbw
	 * @param string $dbName
	 * @return array[tableName][keyType][columnName]
	 */
	static public function getTablesWithoutPrimaryKeys(
		Database $dbw, $dbName
	) {
		
	}

	/**
	 * Return a list of tables that use the MyISAM engine.
	 *
	 * @param Database $dbw
	 * @param string $dbName
	 * @return array[]
	 */
	static public function getMyISAMTables(
		Database $dbw, $dbName
	) {
		
	}

	static public function ensureNoMyISAMTables(
		DatabaseUpdater $upd
	) {
		$dbName = $upd->getDBName();
		$dbw = $upd->getDatabase();
		$tables = self::getMyISAMTables( $dbw, $dbName );
		if ( count( $tables ) ) {
			$upd->ouput( "Converting tables to InnoDB...\n" );
			foreach ( $tables as $table ) {
				self::convertToInnoDB( $dbw, $table );
			}
		}
	}

	static public function ensurePrimaryKeys(
		DatabaseUpdater $upd
	) {
		$dbName = $upd->getDBName();
		$dbw = $upd->getDatabase();
		$tables = self::getTablesWithoutPrimaryKeys( $dbw, $dbName );
		if ( count( $tables ) ) {
			$upd->output( "Updating tables to have a primary key...\n" );
			foreach ( array_keys( $tables ) as $table ) {
				self::addPrimaryKey( $upd, $table, $tables );
			}
		}
	}
}
