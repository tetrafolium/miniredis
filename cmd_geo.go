// Commands from https://redis.io/commands#generic

package miniredis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsGeo handles GEOADD, GEORADIUS etc.
func commandsGeo(m *Miniredis) {
	m.srv.Register("GEOADD", m.cmdGeoadd)
	m.srv.Register("GEOPOS", m.cmdGeopos)
	m.srv.Register("GEORADIUS", m.cmdGeoradius)
}

// GEOADD
func (m *Miniredis) cmdGeoadd(c *server.Peer, cmd string, args []string) {
	if len(args) < 3 || len(args[1:])%3 != 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}
	key, args := args[0], args[1:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "zset" {
			c.WriteError(ErrWrongType.Error())
			return
		}

		toSet := map[string]float64{}
		for len(args) > 2 {
			rawLong, rawLat, name := args[0], args[1], args[2]
			args = args[3:]
			longitude, err := strconv.ParseFloat(rawLong, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}
			latitude, err := strconv.ParseFloat(rawLat, 64)
			if err != nil {
				c.WriteError("ERR value is not a valid float")
				return
			}

			if latitude < -85.05112878 ||
				latitude > 85.05112878 ||
				longitude < -180 ||
				longitude > 180 {
				c.WriteError(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", longitude, latitude))
				return
			}

			toSet[name] = float64(toGeohash(longitude, latitude))
		}

		set := 0
		for name, score := range toSet {
			if db.ssetAdd(key, score, name) {
				set++
			}
		}
		c.WriteInt(set)
	})
}

// GEOPOS
func (m *Miniredis) cmdGeopos(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}
	key, args := args[0], args[1:]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "zset" {
			c.WriteError(ErrWrongType.Error())
			return
		}

		c.WriteLen(len(args))
		for _, l := range args {
			if !db.ssetExists(key, l) {
				c.WriteNull()
				continue
			}
			score := db.ssetScore(key, l)
			c.WriteLen(2)
			long, lat := fromGeohash(uint64(score))
			c.WriteBulk(formatGeo(long))
			c.WriteBulk(formatGeo(lat))
		}
	})
}

type geoDistance struct {
	Name      string
	Distance  float64
	Longitude float64
	Latitude  float64
}

func (m *Miniredis) cmdGeoradius(c *server.Peer, cmd string, args []string) {
	if len(args) < 5 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}

	key := args[0]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	radius, err := strconv.ParseFloat(args[3], 64)
	if err != nil || radius < 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	toMeter := 1.0
	switch args[4] {
	case "m":
		toMeter = 1
	case "km":
		toMeter = 1000
	case "mi":
		toMeter = 1609.34
	case "ft":
		toMeter = 0.3048
	default:
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	var (
		withDist  = false
		withCoord = false
	)
	for _, arg := range args[4:] {
		switch strings.ToUpper(arg) {
		case "WITHCOORD":
			withCoord = true
		case "WITHDIST":
			withDist = true
		}
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		members := db.ssetElements(key)

		matches := withinRadius(members, longitude, latitude, radius*toMeter)

		c.WriteLen(len(matches))
		for _, member := range matches {
			if !withDist && !withCoord {
				c.WriteBulk(member.Name)
				continue
			}

			len := 1
			if withDist {
				len++
			}
			if withCoord {
				len++
			}
			c.WriteLen(len)
			c.WriteBulk(member.Name)
			if withDist {
				c.WriteBulk(fmt.Sprintf("%.4f", member.Distance/toMeter))
			}
			if withCoord {
				c.WriteLen(2)
				c.WriteBulk(formatGeo(member.Longitude))
				c.WriteBulk(formatGeo(member.Latitude))
			}
		}
	})
}

func withinRadius(members []ssElem, longitude, latitude, radius float64) []geoDistance {
	matches := []geoDistance{}
	for _, el := range members {
		elLo, elLat := fromGeohash(uint64(el.score))
		distanceInMeter := distance(latitude, longitude, elLat, elLo)

		if distanceInMeter <= radius {
			matches = append(matches, geoDistance{
				Name:      el.member,
				Distance:  distanceInMeter,
				Longitude: elLo,
				Latitude:  elLat,
			})
		}
	}
	return matches
}
